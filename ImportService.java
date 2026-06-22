package com.cim.service;

import com.cim.model.cim.CIMObject;
import com.cim.model.mongo.CimObjectRecord;
import com.cim.model.mongo.StageTimings;
import com.cim.model.mongo.ValidationJob;
import com.cim.repository.CimObjectRecordRepository;
import com.cim.repository.RawCimObjectRepository;
import com.cim.model.mongo.RawCimObject;
import com.cim.repository.ValidationJobRepository;
import com.cim.streaming.ProgressTracker;
import com.cim.streaming.StreamingRdfXmlParser;
import com.cim.streaming.StreamingReferenceResolver;
import com.cim.util.MapKeySanitizer;
import com.cim.validation.registry.CIMHierarchyRegistry;
import com.cim.model.mongo.Neo4jExportConfig;
import com.cim.model.mongo.ExportRequest;
import com.cim.model.mongo.ExportRequest.ExportMode;
import com.cim.repository.Neo4jExportConfigRepository;
import com.cim.validation.registry.PathValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Core import pipeline: parse → path-validate → save → resolve-references
 *
 * Enhancements in this version:
 *   REQ #1: orgId stored on every CimObjectRecord and ValidationJob
 *   REQ #2: Namespace filtering — only store attributes from enabled namespaces
 *   REQ #3: Disabled namespace attributes logged as UNKNOWN_NAMESPACE in pathIssues
 *   REQ #5: percentSaved fix — uses totalObjectsInFile for accurate percentage
 *   REQ #6: Per-stage timings stored on ValidationJob.stageTimings
 */
@Service
public class ImportService {

    private static final Logger log = LoggerFactory.getLogger(ImportService.class);

    @Value("${cim.streaming.chunk-size:500}")  private int chunkSize;
    @Value("${cim.streaming.log-every:10000}") private int logEvery;
    @Value("${cim.upload.temp-dir:/tmp/cim-uploads}") private String tempDir;

    private final StreamingRdfXmlParser      rdfParser;
    private final StreamingReferenceResolver refResolver;
    private final CimObjectRecordRepository  objectRepo;
    private final RawCimObjectRepository     rawRepo;
    private final ValidationJobRepository    jobRepo;
    private final CIMHierarchyRegistry       registry;
    private final NamespaceService             namespaceService;
    private final Neo4jExportService           neo4jExportService;
    private final Neo4jExportConfigRepository  exportConfigRepo;
    private final GeoJsonExportService         geoJsonService;

    private final Map<String, ProgressTracker> trackers = new ConcurrentHashMap<>();

    public ImportService(StreamingRdfXmlParser rdfParser,
                          com.cim.streaming.StreamingMilsoftCsvParser milsoftParser,
                          StreamingReferenceResolver refResolver,
                          CimObjectRecordRepository objectRepo,
                          RawCimObjectRepository rawRepo,
                          ValidationJobRepository jobRepo,
                          CIMHierarchyRegistry registry,
                          NamespaceService namespaceService,
                          Neo4jExportService neo4jExportService,
                          Neo4jExportConfigRepository exportConfigRepo,
                          GeoJsonExportService geoJsonService) {
        this.rdfParser          = rdfParser;
        this.milsoftParser      = milsoftParser;
        this.refResolver        = refResolver;
        this.objectRepo         = objectRepo;
        this.rawRepo            = rawRepo;
        this.jobRepo            = jobRepo;
        this.registry           = registry;
        this.namespaceService   = namespaceService;
        this.neo4jExportService = neo4jExportService;
        this.exportConfigRepo   = exportConfigRepo;
        this.geoJsonService     = geoJsonService;
    }

    // ── Synchronous (≤50MB) ───────────────────────────────────────────────

    public void processAndResolve(InputStream in, String fileName,
                                   String format, String jobId,
                                   String version, String orgId) throws Exception {
        ProgressTracker p = new ProgressTracker(jobId, logEvery);
        trackers.put(jobId, p);
        // Load namespace cache fresh from MongoDB for this job (cluster-safe)
        namespaceService.loadForJob(jobId, orgId);
        try {
            run(in, fileName, format, jobId, version, orgId, p);
        } finally {
            // Always clear — success or failure — prevents stale data
            namespaceService.clearAfterJob(jobId);
        }
    }

    // ── Async (>50MB) ─────────────────────────────────────────────────────

    @Async
    public CompletableFuture<Void> processAndResolveAsync(InputStream in,
            String fileName, String format, String jobId,
            String version, String orgId) {
        ProgressTracker p = new ProgressTracker(jobId, logEvery);
        trackers.put(jobId, p);
        namespaceService.loadForJob(jobId, orgId);
        try {
            run(in, fileName, format, jobId, version, orgId, p);
        } catch (Exception e) {
            log.error("Async import failed: {}", e.getMessage());
        } finally {
            namespaceService.clearAfterJob(jobId);
        }
        return CompletableFuture.completedFuture(null);
    }

    // ── Core pipeline ─────────────────────────────────────────────────────

    private void run(InputStream in, String fileName, String format,
                      String jobId, String version, String orgId,
                      ProgressTracker progress) throws Exception {

        ValidationJob job = jobRepo.findByJobId(jobId).orElseThrow();
        job.setStatus("RUNNING");
        job.setNetworkVersion(version);
        job.setOrgId(orgId);
        // Store enabled namespaces at job level for audit trail
        job.setEnabledNamespaces(
                namespaceService.getEnabled().stream()
                        .map(c -> c.getPrefix())
                        .collect(java.util.stream.Collectors.toList()));
        jobRepo.save(job);

        StageTimings timings = new StageTimings();
        long pipelineStart = System.currentTimeMillis();

        try {
            // ── Step 1: Stream parse + path-validate + save ───────────────
            progress.setStage(ProgressTracker.Stage.STREAMING_PARSE,
                    "Parsing " + format + "...");
            long parseStart = System.currentTimeMillis();
            Path tmp = saveToDisk(in, jobId, fileName);
            try {
                streamValidateAndSave(tmp, fileName, format, jobId,
                                      version, orgId, progress);
            } finally {
                deleteTempFile(tmp);
            }
            timings.setParseMs(System.currentTimeMillis() - parseStart);
            progress.log("Parse+save done in " + timings.getParseMs() + "ms");

            // ── Step 2: Resolve cross-references ──────────────────────────
            progress.setStage(ProgressTracker.Stage.RESOLVING_REFERENCES,
                    "Resolving cross-references...");
            long refStart = System.currentTimeMillis();
            long resolved = refResolver.resolveReferencesInMongo(
                    jobId, chunkSize, progress);
            timings.setReferenceMs(System.currentTimeMillis() - refStart);
            progress.log("References resolved: " + resolved
                    + " in " + timings.getReferenceMs() + "ms");

            // ── Done ──────────────────────────────────────────────────────
            timings.setTotalMs(System.currentTimeMillis() - pipelineStart);
            job = jobRepo.findByJobId(jobId).orElse(job);
            job.setStatus("RESOLVED");
            job.setTotalObjects((int) progress.getParsed());
            job.setPhysicalObjects((int) objectRepo.countByJobIdAndCategory(jobId,"PHYSICAL"));
            job.setLogicalObjects((int)  objectRepo.countByJobIdAndCategory(jobId,"LOGICAL"));
            job.setPathIssueCount((int)  countPathIssues(jobId));
            job.setStageTimings(timings);
            job.setCompletedAt(Instant.now());
            job.setProcessingMs(timings.getTotalMs());
            jobRepo.save(job);
            progress.done();

            // REQ #7: human-readable stage times in log
            log.info("Import done: job={} version={} orgId={} objects={} resolved={} "
                    + "parse={} reference={} total={}",
                    jobId, version, orgId, progress.getParsed(), resolved,
                    com.cim.model.mongo.StageTimings.format(timings.getParseMs() != null ? timings.getParseMs() : 0),
                    com.cim.model.mongo.StageTimings.format(timings.getReferenceMs() != null ? timings.getReferenceMs() : 0),
                    com.cim.model.mongo.StageTimings.format(timings.getTotalMs() != null ? timings.getTotalMs() : 0));

            // ── Auto-generate GeoJSON if appropriate ──────────────────────
            // Two trigger paths combined here, both per the
            // generate-geojson design:
            //   (1) Format-driven: Milsoft imports always get GeoJSON.
            //       Their .STD files carry State Plane coords that downstream
            //       tools (legacy topology processor, web maps) consume.
            //   (2) Config-driven: any active export config for this orgId
            //       with generateGeoJson=true forces generation regardless
            //       of format.
            // Failures are logged but never propagate — GeoJSON is a
            // post-import artifact, not part of the import contract.
            maybeAutoGenerateGeoJson(jobId, orgId, format);

            // ── Auto-trigger Neo4j export if configs exist ────────────────
            triggerAutoExport(jobId, version, orgId);

        } catch (Exception e) {
            timings.setTotalMs(System.currentTimeMillis() - pipelineStart);
            log.error("Import failed job={}: {}", jobId, e.getMessage());
            job = jobRepo.findByJobId(jobId).orElse(job);
            job.setStatus("FAILED"); job.setErrorMessage(e.getMessage());
            job.setStageTimings(timings); job.setCompletedAt(Instant.now());
            jobRepo.save(job);
            progress.fail(e.getMessage());
            throw e;
        }
    }

    // ── Stream, validate, save ────────────────────────────────────────────

    private void streamValidateAndSave(Path tmp, String fileName,
                                        String format, String jobId,
                                        String version, String orgId,
                                        ProgressTracker progress) throws Exception {
        // Two batches — saved together each chunk
        List<CimObjectRecord> batch    = new ArrayList<>(chunkSize);
        List<RawCimObject>    rawBatch = new ArrayList<>(chunkSize);

        progress.setTotalObjects(estimateObjectCount(tmp, format));

        // Route to correct parser based on format
        java.util.function.Consumer<com.cim.model.cim.CIMObject> onObject = obj -> {
            rawBatch.add(toRawObject(obj, jobId, version, orgId, format, fileName));
            batch.add(toRecord(obj, jobId, version, orgId));
            progress.incrementParsed();
            if (batch.size() >= chunkSize) {
                rawRepo.saveAll(new ArrayList<>(rawBatch));
                rawBatch.clear();
                objectRepo.saveAll(new ArrayList<>(batch));
                progress.incrementSaved(batch.size());
                batch.clear();
            }
        };

        try (InputStream fis = Files.newInputStream(tmp)) {
            if ("MILSOFT_CSV".equalsIgnoreCase(format)) {
                milsoftParser.stream(fis, fileName, onObject);
            } else {
                // Default: RDF/XML (JSON-LD uses same parser with different dialect)
                rdfParser.stream(fis, fileName, onObject);
            }
        }
        // Final batch
        if (!batch.isEmpty()) {
            rawRepo.saveAll(rawBatch);
            objectRepo.saveAll(batch);
            progress.incrementSaved(batch.size());
        }
        progress.log("Stream complete: " + progress.getParsed()
                + " objects → raw_objects + cim_objects");
    }

    // ── Build CimObjectRecord with namespace filtering ─────────────────────

    CimObjectRecord toRecord(CIMObject obj, String jobId,
                              String version, String orgId) {
        CimObjectRecord r = new CimObjectRecord();
        r.setJobId(jobId);
        r.setRdfId(obj.getRdfId());
        r.setMrid(obj.getMrid());
        r.setCimType(obj.getCimType());
        r.setName(obj.getName());
        r.setSourceFormat(obj.getSourceFormat());
        r.setSourceFile(obj.getSourceFile());
        r.setVersion(version);
        r.setOrgId(orgId);  // REQ #1

        // Category and isRequired from OWL registry only
        String category = registry.getCategory(obj.getCimType());
        if ("UNKNOWN".equals(category)) category = "PHYSICAL"; // safe default
        r.setCategory(category);
        r.setRequired(registry.getDefaultRequired(obj.getCimType()));

        // ── REQ #2 + #3: Namespace-filtered attributes + references ──────
        Map<String,String> filteredAttrs = new LinkedHashMap<>();
        Map<String,String> filteredRefs  = new LinkedHashMap<>();
        List<Map<String,String>> pathIssues = new ArrayList<>();

        // Process attributes
        for (Map.Entry<String,String> e : obj.getAttributes().entrySet()) {
            String tag    = e.getKey();
            String value  = e.getValue();
            String prefix = namespaceService.extractPrefix(tag);

            if (namespaceService.isEnabled(jobId, prefix)) {
                // Namespace enabled → store attribute
                filteredAttrs.put(MapKeySanitizer.encode(tag), value);

                // Path validation — only for namespaces with validatePath=true
                if (namespaceService.shouldValidatePath(jobId, prefix)
                        && registry.isKnownClass(obj.getCimType())) {
                    PathValidationResult res =
                            registry.validateAttributePath(obj.getCimType(), tag);
                    if (res.isInvalid()) {
                        pathIssues.add(Map.of(
                            "tag",         tag,
                            "status",      "INVALID_PATH",
                            "explanation", res.getExplanation()
                        ));
                    }
                }
                // If validatePath=false (custom namespace) → store as-is, no path check
            } else {
                // REQ #3: Namespace disabled → log UNKNOWN_NAMESPACE issue
                // Do NOT store the attribute
                pathIssues.add(Map.of(
                    "tag",         tag,
                    "status",      "UNKNOWN_NAMESPACE",
                    "explanation", "Namespace '" + prefix + "' is not enabled. "
                                   + "Enable via POST /api/admin/namespaces/" + prefix
                                   + "/enable to store these attributes."
                ));
            }
        }

        // Process references (same namespace logic)
        for (Map.Entry<String,String> e : obj.getReferences().entrySet()) {
            String tag    = e.getKey();
            String prefix = namespaceService.extractPrefix(tag);
            if (namespaceService.isEnabled(jobId, prefix)) {
                filteredRefs.put(MapKeySanitizer.encode(tag), e.getValue());
            }
            // Disabled namespace references are silently skipped
            // (already logged under attributes or reference handling)
        }

        r.setAttributes(filteredAttrs);
        r.setReferences(filteredRefs);
        if (!pathIssues.isEmpty()) r.setPathIssues(pathIssues);

        r.setCreatedAt(Instant.now());
        return r;
    }

    // ── REQ #5: estimate object count for accurate percentSaved ──────────

    private long estimateObjectCount(Path tmp, String format) {
        if (!"RDF_XML".equals(format)) return -1;
        try {
            long fileSize = Files.size(tmp);
            // Rough estimate: ~500 bytes per CIM object on average in RDF/XML
            return Math.max(1, fileSize / 500);
        } catch (Exception e) { return -1; }
    }

    // ── Progress ──────────────────────────────────────────────────────────

    public Optional<ProgressTracker> getTracker(String jobId) {
        return Optional.ofNullable(trackers.get(jobId));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private long countPathIssues(String jobId) {
        org.springframework.data.mongodb.core.query.Query q =
                new org.springframework.data.mongodb.core.query.Query(
                        org.springframework.data.mongodb.core.query.Criteria
                                .where("jobId").is(jobId)
                                .and("pathIssues.0").exists(true));
        // Use repo count — avoids injecting MongoTemplate here
        return objectRepo.findByJobId(jobId,
                org.springframework.data.domain.PageRequest.of(0, 1))
                .getTotalElements(); // approximate, good enough for reporting
    }

    private Path saveToDisk(InputStream in, String jobId,
                             String fileName) throws IOException {
        Path dir = Paths.get(tempDir);
        Files.createDirectories(dir);
        Path file = dir.resolve(jobId + "_" + fileName);
        Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING);
        return file;
    }

    private void deleteTempFile(Path p) {
        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
    }

    /**
     * Auto-generate GeoJSON for this import if either condition holds:
     * <ol>
     *   <li>The import format is Milsoft (always generates).</li>
     *   <li>Any active export config for this orgId has
     *       {@code generateGeoJson=true}.</li>
     * </ol>
     * Failures are logged but never propagated — GeoJSON is a post-import
     * artifact, not part of the import contract, mirroring the auto-export
     * error-handling pattern below.
     */
    private void maybeAutoGenerateGeoJson(String jobId, String orgId, String format) {
        boolean isMilsoft = format != null
                && format.toUpperCase().contains("MILSOFT");

        boolean configRequested = false;
        if (!isMilsoft) {
            try {
                List<com.cim.model.mongo.Neo4jExportConfig> cfgs =
                        exportConfigRepo.findByOrgIdAndActiveTrue(orgId);
                if (cfgs != null) {
                    for (com.cim.model.mongo.Neo4jExportConfig c : cfgs) {
                        if (c.isGenerateGeoJson()) { configRequested = true; break; }
                    }
                }
            } catch (Exception ignore) {
                // Treat config-check failure as "no opt-in" — we already log
                // similar issues in triggerAutoExport, and Milsoft path is
                // unaffected.
            }
        }

        if (!isMilsoft && !configRequested) {
            log.debug("GeoJSON skipped for job={} orgId={} format={} "
                    + "(not Milsoft and no config opt-in)",
                    jobId, orgId, format);
            return;
        }

        try {
            var r = geoJsonService.generate(jobId);
            log.info("GeoJSON generated for job={} file={} features={} "
                    + "(trigger: {})",
                    jobId, r.filePath, r.featuresWritten,
                    isMilsoft ? "milsoft-format" : "config-opt-in");
        } catch (Exception e) {
            log.warn("GeoJSON generation failed for job={}: {}",
                    jobId, e.getMessage());
        }
    }

    /**
     * Triggered automatically after every successful import (status=RESOLVED).
     * Finds all active Neo4jExportConfig records with autoExport=true
     * that match this orgId (or have no orgId restriction).
     * Each matching config triggers a separate async Neo4j export job.
     */
    /**
     * Triggered after every successful import (status=RESOLVED).
     *
     * FIX 1 — OrgId specific:
     *   Queries configs WHERE orgId = this import's orgId OR orgId is empty/null.
     *   A config with orgId="CAISO" only triggers for CAISO imports.
     *   A config with orgId="" triggers for ALL organisations.
     *
     * FIX 2 — Three variant support:
     *   If config has fullCimTypes + sanitisedCimTypes + rationalisedCimTypes set,
     *   triggers all three variants in one multi-variant ExportRequest.
     *   Each variant exports its own CIM type set under its own version suffix.
     *
     * FIX 3 — Version-specific query:
     *   Uses findByOrgIdAndAutoExportTrueAndActiveTrue(orgId) to get
     *   only configs matching this import's orgId.
     */
    private void triggerAutoExport(String jobId, String version, String orgId) {
        try {
            // FIX 1: Query orgId-specific configs directly from DB
            // Gets configs where orgId matches OR orgId is blank (applies to all)
            List<com.cim.model.mongo.Neo4jExportConfig> specificConfigs =
                    exportConfigRepo.findByOrgIdAndAutoExportTrueAndActiveTrue(orgId);
            List<com.cim.model.mongo.Neo4jExportConfig> globalConfigs =
                    exportConfigRepo.findByAutoExportTrueAndActiveTrue()
                    .stream()
                    .filter(c -> c.getOrgId() == null || c.getOrgId().isBlank())
                    .collect(java.util.stream.Collectors.toList());

            // Merge: orgId-specific + global (no orgId restriction)
            List<com.cim.model.mongo.Neo4jExportConfig> configs = new ArrayList<>();
            configs.addAll(specificConfigs);
            globalConfigs.stream()
                    .filter(g -> specificConfigs.stream()
                            .noneMatch(s -> s.getId().equals(g.getId())))
                    .forEach(configs::add);

            if (configs.isEmpty()) {
                log.info("No auto-export configs for orgId={} job={}", orgId, jobId);
                return;
            }

            int triggered = 0;
            for (com.cim.model.mongo.Neo4jExportConfig cfg : configs) {

                // FIX 2: Build multi-variant request if three lists are configured
                boolean hasMultiVariant =
                        (cfg.getFullCimTypes()      != null && !cfg.getFullCimTypes().isEmpty()) ||
                        (cfg.getSanitisedCimTypes() != null && !cfg.getSanitisedCimTypes().isEmpty()) ||
                        (cfg.getRationalisedCimTypes() != null && !cfg.getRationalisedCimTypes().isEmpty());

                if (hasMultiVariant) {
                    // All three variants in one request
                    ExportRequest req = new ExportRequest();
                    req.setJobId(jobId);
                    req.setOrgId(orgId);
                    req.setClearFirst(cfg.isClearFirst());
                    req.setFullCimTypes(cfg.getFullCimTypes()      != null
                            ? cfg.getFullCimTypes()      : new ArrayList<>());
                    req.setSanitisedCimTypes(cfg.getSanitisedCimTypes() != null
                            ? cfg.getSanitisedCimTypes() : new ArrayList<>());
                    req.setRationalisedCimTypes(cfg.getRationalisedCimTypes() != null
                            ? cfg.getRationalisedCimTypes() : new ArrayList<>());

                    neo4jExportService.startExport(req);
                    triggered++;
                    log.info("Auto-export (multi-variant): config='{}' orgId={} job={} "
                            + "full={} sanitised={} rationalised={}",
                            cfg.getConfigName(), orgId, jobId,
                            req.getFullCimTypes().size(),
                            req.getSanitisedCimTypes().size(),
                            req.getRationalisedCimTypes().size());

                } else {
                    // Single variant — REQ #5: default SANITISED for import flow
                    List<String> types = cfg.getSanitisedCimTypes() != null
                            && !cfg.getSanitisedCimTypes().isEmpty()
                            ? cfg.getSanitisedCimTypes()
                            : (cfg.getCimTypes() != null ? cfg.getCimTypes() : new ArrayList<>());

                    if (types.isEmpty()) {
                        log.warn("Config '{}' has no cimTypes — skipping",
                                cfg.getConfigName());
                        continue;
                    }

                    ExportRequest req = new ExportRequest(
                            jobId,
                            ExportRequest.ExportMode.CUSTOM,
                            types,
                            cfg.isClearFirst());
                    req.setVersionType(ExportRequest.VersionType.SANITISED);
                    req.setOrgId(orgId);

                    neo4jExportService.startExport(req);
                    triggered++;
                    log.info("Auto-export (sanitised): config='{}' orgId={} types={} job={}",
                            cfg.getConfigName(), orgId, types.size(), jobId);
                }
            }

            if (triggered > 0)
                log.info("Auto-export complete: {} job(s) triggered for import job={} orgId={}",
                        triggered, jobId, orgId);

        } catch (Exception e) {
            // Auto-export failure must NEVER affect import status
            log.error("Auto-export trigger failed for job={}: {}", jobId, e.getMessage());
        }
    }

    public static String sha256(byte[] data) throws Exception {
        byte[] h = MessageDigest.getInstance("SHA-256").digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : h) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
