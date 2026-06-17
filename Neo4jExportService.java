package com.cim.service;

import com.cim.model.mongo.CimObjectRecord;
import com.cim.model.mongo.Neo4jExportJob;
import com.cim.model.mongo.ExportRequest;
import com.cim.model.mongo.ExportRequest.ExportMode;
import com.cim.model.mongo.ExportRequest.VersionType;
import com.cim.repository.CimObjectRecordRepository;
import com.cim.repository.Neo4jExportJobRepository;
import com.cim.repository.ValidationJobRepository;
import com.cim.repository.Neo4jExportConfigRepository;
import com.cim.model.mongo.Neo4jExportConfig;
import com.cim.util.MapKeySanitizer;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * Neo4j Export Service — v5
 *
 * Changes from previous version:
 *
 * REQ #1 — Index drop/rebuild removed.
 *           Indexes are maintained live. Heap tuning handles performance.
 *
 * REQ #2 — No :CIMNode or :CIMObject labels.
 *           Nodes have ONLY their specific cimType label (:ACLineSegment etc).
 *           Matches your existing Neo4j topology structure.
 *
 * REQ #3 — Three version types per export:
 *           FULL      → stored with version "{version}-0"
 *           SANITISED → stored with version "{version}-1" (default)
 *           RATIONALISED → stored with version "{version}-2"
 *
 * REQ #4 — OrgId specific: export uses jobId orgId + req.orgId override.
 *
 * Performance: per-label constraints created upfront, batch=5000.
 */
@Service
public class Neo4jExportService {

    private static final Logger log = LoggerFactory.getLogger(Neo4jExportService.class);

    @Value("${cim.neo4j.export.batch-size:5000}")
    private int batchSize;

    private final Driver                     driver;
    private final MongoTemplate              mongo;
    private final CimObjectRecordRepository  objectRepo;
    private final Neo4jExportJobRepository   exportRepo;
    private final ValidationJobRepository    jobRepo;
    private final Neo4jExportConfigRepository configRepo;

    // Tracks labels that already have a constraint this export run
    private final Set<String> constrainedLabels = new HashSet<>();

    private Neo4jExportExecutor executor;

    @org.springframework.beans.factory.annotation.Autowired
    public void setExecutor(Neo4jExportExecutor executor) { this.executor = executor; }

    public Neo4jExportService(Driver driver, MongoTemplate mongo,
                               CimObjectRecordRepository objectRepo,
                               Neo4jExportJobRepository exportRepo,
                               ValidationJobRepository jobRepo,
                               Neo4jExportConfigRepository configRepo) {
        this.driver      = driver;
        this.mongo       = mongo;
        this.objectRepo  = objectRepo;
        this.exportRepo  = exportRepo;
        this.jobRepo     = jobRepo;
        this.configRepo  = configRepo;
    }

    // ── Start export ──────────────────────────────────────────────────────

    /**
     * Starts a Neo4j export.
     *
     * SINGLE variant:   uses req.versionType + req.cimTypes
     * MULTI variant:    uses req.fullCimTypes + sanitisedCimTypes + rationalisedCimTypes
     *                   triggers three separate async export jobs
     *
     * Returns the first job created (or a summary job for multi-variant).
     */
    public Neo4jExportJob startExport(ExportRequest req) {
        String jobOrgId = jobRepo.findByJobId(req.getJobId())
                .map(j -> j.getOrgId() != null ? j.getOrgId() : "").orElse("");
        String orgId = (req.getOrgId() != null && !req.getOrgId().isBlank())
                       ? req.getOrgId().trim() : jobOrgId;

        String networkVersion = jobRepo.findByJobId(req.getJobId())
                .map(j -> j.getNetworkVersion() != null ? j.getNetworkVersion() : "")
                .orElse("");

        if (req.isMultiVariant()) {
            return startMultiVariantExport(req, networkVersion, orgId);
        }

        // FROM_CONFIG: load cimTypes from stored Neo4jExportConfig
        if (req.getMode() == ExportRequest.ExportMode.FROM_CONFIG) {
            return startFromConfig(req, networkVersion, orgId);
        }

        // Single variant with explicit cimTypes in request
        return startSingleExport(req, networkVersion, orgId,
                req.getVersionType() != null
                        ? req.getVersionType() : ExportRequest.VersionType.SANITISED,
                req.getCimTypes());
    }

    /**
     * FROM_CONFIG: load the stored Neo4jExportConfig and decide:
     *
     *   Case 1 — versionType NOT set (null) OR all three lists populated in config:
     *     → Trigger ALL configured variants as multi-variant export.
     *     → FULL(-0) triggered if config.fullCimTypes is not empty.
     *     → SANITISED(-1) triggered if config.sanitisedCimTypes is not empty.
     *     → RATIONALISED(-2) triggered if config.rationalisedCimTypes is not empty.
     *
     *   Case 2 — versionType IS explicitly set (FULL / SANITISED / RATIONALISED):
     *     → Trigger only that specific variant.
     *     → e.g. versionType=RATIONALISED → only config.rationalisedCimTypes → {version}-2
     *
     * This way:
     *   triggerExport=true in revalidation (no versionType) → all three variants
     *   Manual export with versionType=SANITISED → only sanitised
     */
    private Neo4jExportJob startFromConfig(ExportRequest req,
                                            String networkVersion, String orgId) {
        if (req.getConfigId() == null || req.getConfigId().isBlank())
            throw new IllegalArgumentException(
                    "configId required when mode=FROM_CONFIG");

        Neo4jExportConfig cfg = configRepo.findById(req.getConfigId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Neo4jExportConfig not found: " + req.getConfigId()));

        boolean hasSpecificVariant = req.getVersionType() != null;

        if (hasSpecificVariant) {
            // Case 2: caller wants only one specific variant
            List<String> types = getTypesForVariant(cfg, req.getVersionType());
            if (types.isEmpty())
                throw new IllegalArgumentException(
                        "Config '" + cfg.getConfigName() + "' has no cimTypes for variant "
                        + req.getVersionType());
            log.info("FROM_CONFIG single variant: config='{}' variant={} types={}",
                    cfg.getConfigName(), req.getVersionType(), types.size());
            return startSingleExport(req, networkVersion, orgId, req.getVersionType(), types);
        }

        // Case 1: no versionType specified → trigger ALL configured variants
        // Build a multi-variant request from the stored config lists
        ExportRequest multiReq = new ExportRequest();
        multiReq.setJobId(req.getJobId());
        multiReq.setOrgId(orgId);
        multiReq.setClearFirst(req.isClearFirst());

        if (cfg.getFullCimTypes()           != null && !cfg.getFullCimTypes().isEmpty())
            multiReq.setFullCimTypes(cfg.getFullCimTypes());
        if (cfg.getSanitisedCimTypes()      != null && !cfg.getSanitisedCimTypes().isEmpty())
            multiReq.setSanitisedCimTypes(cfg.getSanitisedCimTypes());
        if (cfg.getRationalisedCimTypes()   != null && !cfg.getRationalisedCimTypes().isEmpty())
            multiReq.setRationalisedCimTypes(cfg.getRationalisedCimTypes());
        if (cfg.getCustomCimTypes()         != null && !cfg.getCustomCimTypes().isEmpty()) {
            multiReq.setCustomCimTypes(cfg.getCustomCimTypes());
            if (cfg.getCustomNeoVersion()   != null && !cfg.getCustomNeoVersion().isBlank())
                multiReq.setNeoVersion(cfg.getCustomNeoVersion());
        }

        if (!multiReq.isMultiVariant())
            throw new IllegalArgumentException(
                    "Config '" + cfg.getConfigName()
                    + "' has no cimTypes configured in any variant list.");

        log.info("FROM_CONFIG multi-variant: config='{}' full={} sanitised={} rationalised={}",
                cfg.getConfigName(),
                multiReq.getFullCimTypes().size(),
                multiReq.getSanitisedCimTypes().size(),
                multiReq.getRationalisedCimTypes().size());

        return startMultiVariantExport(multiReq, networkVersion, orgId);
    }

    private List<String> getTypesForVariant(Neo4jExportConfig cfg,
                                             ExportRequest.VersionType vType) {
        if (vType == ExportRequest.VersionType.FULL)
            return cfg.getFullCimTypes()      != null ? cfg.getFullCimTypes()      : new ArrayList<>();
        if (vType == ExportRequest.VersionType.RATIONALISED)
            return cfg.getRationalisedCimTypes() != null ? cfg.getRationalisedCimTypes() : new ArrayList<>();
        return cfg.getSanitisedCimTypes()     != null ? cfg.getSanitisedCimTypes() : new ArrayList<>();
    }

    /**
     * Triggers three separate export jobs — one per variant.
     * Each uses its own cimType list and version suffix.
     */
    private Neo4jExportJob startMultiVariantExport(ExportRequest req,
                                                    String networkVersion,
                                                    String orgId) {
        Neo4jExportJob firstJob = null;

        // FULL (-0)
        if (!req.getFullCimTypes().isEmpty()) {
            Neo4jExportJob j = startSingleExport(req, networkVersion, orgId,
                    ExportRequest.VersionType.FULL, req.getFullCimTypes());
            if (firstJob == null) firstJob = j;
            log.info("Multi-variant: FULL ({}-0) triggered — {} cimTypes",
                    networkVersion, req.getFullCimTypes().size());
        }

        // SANITISED (-1)
        if (!req.getSanitisedCimTypes().isEmpty()) {
            Neo4jExportJob j = startSingleExport(req, networkVersion, orgId,
                    ExportRequest.VersionType.SANITISED, req.getSanitisedCimTypes());
            if (firstJob == null) firstJob = j;
            log.info("Multi-variant: SANITISED ({}-1) triggered — {} cimTypes",
                    networkVersion, req.getSanitisedCimTypes().size());
        }

        // RATIONALISED (-2)
        if (!req.getRationalisedCimTypes().isEmpty()) {
            Neo4jExportJob j = startSingleExport(req, networkVersion, orgId,
                    ExportRequest.VersionType.RATIONALISED, req.getRationalisedCimTypes());
            if (firstJob == null) firstJob = j;
            log.info("Multi-variant: RATIONALISED ({}-2) triggered — {} cimTypes",
                    networkVersion, req.getRationalisedCimTypes().size());
        }

        // CUSTOM — REQ 6: user-provided version string
        if (!req.getCustomCimTypes().isEmpty()) {
            Neo4jExportJob j = startSingleExport(req, networkVersion, orgId,
                    ExportRequest.VersionType.CUSTOM, req.getCustomCimTypes());
            if (firstJob == null) firstJob = j;
            String custVer = req.getNeoVersion() != null ? req.getNeoVersion() : "custom";
            log.info("Multi-variant: CUSTOM ({}) triggered — {} cimTypes",
                    custVer, req.getCustomCimTypes().size());
        }

        return firstJob;
    }

    /**
     * ENHANCEMENT 3: Resolve FROM_CONFIG mode.
     * Loads cimType list from the specified Neo4jExportConfig document.
     */
    private List<String> resolveFromConfig(ExportRequest req,
                                            ExportRequest.VersionType vType) {
        if (req.getConfigId() == null || req.getConfigId().isBlank())
            throw new IllegalArgumentException(
                    "configId required when mode=FROM_CONFIG");
        Neo4jExportConfig cfg = configRepo.findById(req.getConfigId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Neo4jExportConfig not found: " + req.getConfigId()));
        List<String> types;
        if (vType == ExportRequest.VersionType.FULL)
            types = cfg.getFullCimTypes();
        else if (vType == ExportRequest.VersionType.RATIONALISED)
            types = cfg.getRationalisedCimTypes();
        else
            types = cfg.getSanitisedCimTypes();  // SANITISED default

        if (types == null || types.isEmpty())
            throw new IllegalArgumentException(
                    "Config '" + cfg.getConfigName() + "' has no cimTypes for variant "
                    + vType.name());
        log.info("FROM_CONFIG resolved: config='{}' variant={} types={}",
                cfg.getConfigName(), vType, types.size());
        return types;
    }

    private Neo4jExportJob startSingleExport(ExportRequest req,
                                              String networkVersion,
                                              String orgId,
                                              ExportRequest.VersionType vType,
                                              List<String> typesForVariant) {
        String exportId     = UUID.randomUUID().toString();
        String neo4jVersion = req.buildNeo4jVersion(networkVersion, vType);

        Neo4jExportJob job = new Neo4jExportJob(
                exportId, req.getJobId(), neo4jVersion, orgId,
                "CUSTOM_" + vType.name());
        exportRepo.save(job);

        // Build a single-variant request for the executor
        ExportRequest singleReq = new ExportRequest(
                req.getJobId(),
                ExportRequest.ExportMode.CUSTOM,
                typesForVariant,
                req.isClearFirst());
        singleReq.setVersionType(vType);
        singleReq.setOrgId(orgId);

        executor.runAsync(singleReq, job);
        return job;
    }

    // ── Called by executor on async thread ───────────────────────────────

    public void runExport(ExportRequest req, Neo4jExportJob job) {
        try {
            job.setStatus("RUNNING");
            exportRepo.save(job);
            long start = System.currentTimeMillis();
            constrainedLabels.clear();

            // Resolve effective orgId
            String orgId = job.getOrgId();
            // neo4jVersion already has suffix (e.g. "DB140-1")
            String neo4jVersion = job.getNetworkVersion();

            // Clear existing nodes if requested
            long nodesCleared = 0;
            if (req.isClearFirst()) {
                nodesCleared = clearExistingNodesCount(neo4jVersion, orgId);
            }

            // Use CREATE mode only if clearFirst=true AND clear succeeded (or was empty)
            // If clear failed midway, fall back to MERGE to avoid duplicates
            this.useBulkCreate = req.isClearFirst();
            // Note: clearExistingNodes logs warnings on partial failure
            // For production safety, MERGE handles duplicates even if clear was partial

            // REQ 3: Set non-CIM namespace flag from export config if FROM_CONFIG mode
            this.includeNonCimNs = false; // default
            if (req.getMode() == ExportRequest.ExportMode.FROM_CONFIG
                    && req.getConfigId() != null) {
                configRepo.findById(req.getConfigId()).ifPresent(cfg ->
                        this.includeNonCimNs = cfg.isIncludeNonCimNamespaces());
            }
            log.info("includeNonCimNamespaces={}", this.includeNonCimNs);

            // Create per-label constraints upfront
            createConstraintsUpfront(req);

            int[] counts = twoPassExport(req, job, neo4jVersion, orgId);

            job.setStatus("DONE");
            job.setNodesExported(counts[0]);
            job.setRelsExported(counts[1]);
            job.setCompletedAt(Instant.now());
            job.setProcessingMs(System.currentTimeMillis() - start);
            exportRepo.save(job);

            log.info("Export DONE: mode={} version={} orgId={} nodes={} rels={} in {}",
                    req.getMode(), neo4jVersion, orgId, counts[0], counts[1],
                    com.cim.model.mongo.StageTimings.format(
                            System.currentTimeMillis() - start));

        } catch (Exception e) {
            log.error("Export FAILED: {}", e.getMessage(), e);
            job.setStatus("FAILED");
            job.setErrorMessage(e.getMessage());
            job.setCompletedAt(Instant.now());
            exportRepo.save(job);
        }
    }

    // ── Two-pass export ───────────────────────────────────────────────────

    private int[] twoPassExport(ExportRequest req, Neo4jExportJob job,
                                  String neo4jVersion, String orgId) {
        int totalNodes = 0, totalRels = 0;
        long total = mongo.count(new Query(buildCriteria(req)), "cim_objects");
        log.info("Pass 1 — {} nodes batch={} version={} orgId={}",
                total, batchSize, neo4jVersion, orgId);

        long start = System.currentTimeMillis();
        String lastId = null;
        int page = 0;

        // ── Pass 1: Nodes ─────────────────────────────────────────────────
        while (true) {
            List<CimObjectRecord> batch = fetchPage(req, lastId);
            if (batch.isEmpty()) break;

            totalNodes += writeNodes(batch, neo4jVersion, orgId);
            lastId = batch.get(batch.size() - 1).getId();
            page++;

            if (page % 20 == 0) {
                long el   = System.currentTimeMillis() - start;
                long rate = el > 0 ? totalNodes * 1000L / el : 0;
                long eta  = rate > 0 ? (total - totalNodes) / rate / 60 : -1;
                log.info("Pass 1: page={} nodes={}/{} rate={}/s eta={}min",
                        page, totalNodes, total, rate, eta);
                job.setNodesExported(totalNodes);
                exportRepo.save(job);
            }
        }
        log.info("Pass 1 DONE — {} nodes in {}",
                totalNodes,
                com.cim.model.mongo.StageTimings.format(
                        System.currentTimeMillis() - start));

        // ENHANCEMENT 4: Create :OPEN custom nodes for switches with normalOpen=true
        // A switch with normalOpen=true means it is normally in OPEN state.
        // We create a separate :OPEN node connected to the switch to make this
        // clearly visible in the Neo4j graph topology.
        int openNodes = createOpenStateNodes(req.getJobId(), neo4jVersion, orgId);
        if (openNodes > 0)
            log.info("Enhancement 4: {} :OPEN state nodes created", openNodes);
        totalNodes += openNodes;

        // ── Pass 2: Relationships ─────────────────────────────────────────
        log.info("Pass 2 — relationships");
        start = System.currentTimeMillis();
        lastId = null; page = 0;

        // Collapse-through-pass-through topology.
        //
        // CIM models split connectivity across helper objects: an
        // ACLineSegment doesn't reference a ConnectivityNode directly — it
        // references Terminals, and Terminals reference the ConnectivityNode.
        // When the export config omits Terminal (the legacy tool's pattern),
        // we still want edges between equipment and the ConnectivityNodes
        // they really connect to.  This map lets writeRelationships walk
        // through any cimType not in the export config, projecting each
        // pass-through node's outgoing references onto the equipment that
        // points at it.
        //
        // The map is keyed by rdfId and holds (cimType, resolvedRefIds) for
        // every record in this job — both included and excluded cimTypes.
        // Included entries are useful for cycle protection and label-aware
        // edge naming; excluded entries are the actual pass-through hops.
        Set<String> includedCimTypes = new HashSet<>();
        if (req.getCimTypes() != null) includedCimTypes.addAll(req.getCimTypes());
        Map<String, PassThroughEntry> passThroughIndex =
                buildPassThroughIndex(req.getJobId(), includedCimTypes);
        log.info("Pass-through index built: {} records, included cimTypes={}",
                passThroughIndex.size(), includedCimTypes.size());

        while (true) {
            List<CimObjectRecord> batch = fetchPage(req, lastId);
            if (batch.isEmpty()) break;

            totalRels += writeRelationships(batch, neo4jVersion, orgId,
                    includedCimTypes, passThroughIndex);
            lastId = batch.get(batch.size() - 1).getId();
            page++;

            if (page % 20 == 0) {
                log.info("Pass 2: page={} rels={}", page, totalRels);
                job.setRelsExported(totalRels);
                exportRepo.save(job);
            }
        }
        log.info("Pass 2 DONE — {} rels in {}",
                totalRels,
                com.cim.model.mongo.StageTimings.format(
                        System.currentTimeMillis() - start));

        return new int[]{totalNodes, totalRels};
    }

    // ── Pass-through index ────────────────────────────────────────────────
    //
    // Holds enough about each record (its cimType + resolved references) to
    // let writeRelationships walk through nodes that aren't being exported,
    // following their references until landing on a node that IS exported.
    //
    // Memory:  one entry per cim_objects row.  For 116k-row Wake (~30 bytes/
    // entry without the inner lists, plus a few hundred bytes per pass-through
    // row's resolvedRefIds list), this is well under 100MB even at scale.
    private static final class PassThroughEntry {
        final String cimType;
        final Map<String, List<String>> resolved;
        PassThroughEntry(String cimType, Map<String, List<String>> resolved) {
            this.cimType  = cimType;
            this.resolved = resolved;
        }
    }

    private Map<String, PassThroughEntry> buildPassThroughIndex(
            String jobId, Set<String> includedCimTypes) {
        Map<String, PassThroughEntry> idx = new HashMap<>();
        // Stream cim_objects for this job, capture rdfId → (cimType, resolved).
        // We only need cimType and resolvedRefIds for the walk; the projection
        // keeps memory pressure low.
        Query q = new Query(Criteria.where("jobId").is(jobId));
        q.fields().include("rdfId").include("cimType").include("resolvedRefIds");
        // No batch size limit — we want one scan.  MongoTemplate.stream avoids
        // materialising the whole collection in one list.
        try (var stream = mongo.stream(q, CimObjectRecord.class, "cim_objects")) {
            stream.forEach(rec -> {
                if (rec.getRdfId() == null) return;
                Map<String, List<String>> resolved =
                        rec.getResolvedRefIds() != null
                                ? MapKeySanitizer.decodeKeysList(rec.getResolvedRefIds())
                                : new LinkedHashMap<>();
                idx.put(rec.getRdfId(),
                        new PassThroughEntry(rec.getCimType(), resolved));
            });
        }
        return idx;
    }

    // ── Write nodes — single cimType label only (REQ #2) ─────────────────

    private boolean useBulkCreate = false; // set true when clearFirst=true

    /**
     * Write nodes using CREATE (clearFirst) or MERGE (incremental).
     *
     * CREATE mode (clearFirst=true):
     *   - No duplicate check needed — nodes were just deleted
     *   - No index lookup per node — pure append write
     *   - 10-20x faster than MERGE: O(1) vs O(log n) per node
     *   - No MERGE scan degradation as node count grows
     *
     * MERGE mode (incremental):
     *   - Safe when existing nodes may be present
     *   - Uses per-label constraint for O(log n) lookup
     *   - Slower but correct for partial re-exports
     */
    private boolean includeNonCimNs = false; // set from export config before Pass 1

    private int writeNodes(List<CimObjectRecord> records,
                            String neo4jVersion, String orgId) {
        if (records.isEmpty()) return 0;

        Map<String, List<Map<String, Object>>> byLabel = new LinkedHashMap<>();
        for (CimObjectRecord rec : records) {
            String label = sanitizeLabel(rec.getCimType());
            byLabel.computeIfAbsent(label, k -> new ArrayList<>())
                   .add(buildNodeProps(rec, neo4jVersion, orgId));
        }

        int written = 0;
        try (Session s = driver.session()) {
            for (Map.Entry<String, List<Map<String, Object>>> e : byLabel.entrySet()) {
                String label = e.getKey();
                List<Map<String, Object>> nodeBatch = e.getValue();
                String cypher;

                if (useBulkCreate) {
                    // CREATE — pure append, no lookup, no index maintenance during write
                    // :CIMNode is a shared routing label added to ALL nodes.
                    // This enables ONE index (CIMNode.nodeKey) used by Pass 2 MATCH.
                    // Without it, MATCH (src {nodeKey:...}) scans all 953K nodes = slow.
                    // Users still query by specific label (:ACLineSegment etc).
                    cypher = "UNWIND $batch AS p " +
                             "CREATE (n:" + label + ":CIMNode) SET n = p";
                } else {
                    // MERGE — idempotent, uses per-label constraint
                    ensureLabelConstraint(s, label);
                    cypher = "UNWIND $batch AS p " +
                             "MERGE (n:" + label + ":CIMNode {nodeKey: p.nodeKey}) " +
                             "SET n += p";
                }

                final String fc = cypher;
                final List<Map<String, Object>> fb = nodeBatch;
                try {
                    s.writeTransaction(tx -> {
                        tx.run(fc, Values.parameters("batch", fb));
                        return null;
                    });
                    written += nodeBatch.size();
                } catch (Neo4jException ex) {
                    log.warn("Node write failed {}: {}", label, ex.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Session error writeNodes: {}", e.getMessage());
        }
        return written;
    }

    // ── Build node properties — version type variant handling (REQ #3) ────

    private Map<String, Object> buildNodeProps(CimObjectRecord rec,
                                                String neo4jVersion,
                                                String orgId) {
        Map<String, Object> p = new LinkedHashMap<>();
        String r = rec.getRdfId() != null ? rec.getRdfId() : "";
        String o = orgId         != null ? orgId           : "";

        // nodeKey uses neo4jVersion which already has suffix (e.g. "DB140-1")
        p.put("nodeKey",  r + "|" + neo4jVersion + "|" + o);
        p.put("mrid",     rec.getMrid()    != null ? rec.getMrid()    : r);
        p.put("rdfId",    r);
        p.put("name",     rec.getName()    != null ? rec.getName()    : "");
        p.put("cimType",  rec.getCimType() != null ? rec.getCimType() : "Unknown");
        p.put("version",  neo4jVersion);   // "DB140-0", "DB140-1", or "DB140-2"
        p.put("orgId",    o);
        p.put("category", rec.getCategory() != null ? rec.getCategory() : "");
        p.put("uri",      rec.getSourceFile() != null ? rec.getSourceFile() : "");

        // REQ 3: Attribute filtering based on isCimStandard flag in namespace_configs
        // includeNonCimNs=true  → store all attributes (CIM standard + vendor)
        // includeNonCimNs=false → store only CIM standard attributes (no colon prefix)
        if (rec.getAttributes() != null) {
            MapKeySanitizer.decodeKeys(rec.getAttributes()).forEach((k, v) -> {
                if (k == null || v == null || v.isBlank()) return;
                boolean isVendorAttr = k.contains(":");
                if (!isVendorAttr || includeNonCimNs) {
                    p.put(k, v);
                }
            });
        }
        return p;
    }

    // ── Write relationships ───────────────────────────────────────────────

    private int writeRelationships(List<CimObjectRecord> records,
                                    String neo4jVersion, String orgId,
                                    Set<String> includedCimTypes,
                                    Map<String, PassThroughEntry> passThroughIndex) {
        if (records.isEmpty()) return 0;

        Map<String, List<Map<String, Object>>> byRelType = new LinkedHashMap<>();
        for (CimObjectRecord rec : records) {
            if (rec.getResolvedRefIds() == null || rec.getResolvedRefIds().isEmpty()) continue;
            String src = rec.getRdfId() != null ? rec.getRdfId() : "";
            String o   = orgId != null ? orgId : "";

            // resolvedRefIds is Map<String, List<String>> — each CIM property
            // (e.g. ACLineSegment.Terminals) may hold multiple targets.  Emit
            // one Cypher MERGE row per (key, target) pair so multi-valued
            // references produce the correct number of edges in Neo4j.
            //
            // Collapse logic: when a target's cimType is NOT in the exported
            // set, the target node won't exist in Neo4j — emitting an edge to
            // it would silently produce no relationship.  Instead we walk
            // through it: pull the pass-through node's own resolvedRefIds and
            // emit edges to ITS targets, using the original property name as
            // the relType.  This recurses (with cycle protection) until we
            // either land on an included cimType or run out of references.
            MapKeySanitizer.decodeKeysList(rec.getResolvedRefIds())
                    .forEach((key, targets) -> {
                        if (targets == null) return;
                        String relType = "`" + key.replace("`", "") + "`";
                        for (String tgt : targets) {
                            if (tgt == null || tgt.isEmpty()) continue;
                            Set<String> visited = new HashSet<>();
                            visited.add(src);
                            collectEdges(src, tgt, relType, neo4jVersion, o,
                                    includedCimTypes, passThroughIndex,
                                    visited, byRelType, 0);
                        }
                    });
        }

        int written = 0;
        try (Session s = driver.session()) {
            for (Map.Entry<String, List<Map<String, Object>>> e : byRelType.entrySet()) {
                String relType = e.getKey();
                List<Map<String, Object>> relBatch = e.getValue();
                // :CIMNode label + cim_node_nodekey index = O(log n) MATCH
                // Without label: full scan of all 953K nodes per relationship
                // With :CIMNode + index: index lookup, ~1ms per batch
                String cypher =
                    "UNWIND $batch AS r " +
                    "MATCH (src:CIMNode {nodeKey: r.srcKey}) " +
                    "MATCH (tgt:CIMNode {nodeKey: r.tgtKey}) " +
                    "MERGE (src)-[:" + relType +
                    " {version: r.version, orgId: r.orgId}]->(tgt)";
                final String fc = cypher;
                final List<Map<String, Object>> fb = relBatch;
                try {
                    s.writeTransaction(tx -> {
                        tx.run(fc, Values.parameters("batch", fb));
                        return null;
                    });
                    written += relBatch.size();
                } catch (Neo4jException ex) {
                    log.warn("Rel write failed {}: {}", relType, ex.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Session error writeRelationships: {}", e.getMessage());
        }
        return written;
    }

    /**
     * Maximum hops through pass-through nodes before we give up.  In standard
     * CIM, Equipment→Terminal→ConnectivityNode is 2 hops at most.  Three is
     * generous (e.g. Equipment→TransformerEnd→Terminal→ConnectivityNode)
     * and any model needing more hops should add those types to the export
     * config rather than rely on collapse.
     */
    private static final int MAX_COLLAPSE_DEPTH = 4;

    /**
     * Walk from {@code src} via the target {@code tgt}, emitting an edge
     * directly when {@code tgt} is included, or recursing through {@code tgt}'s
     * own resolved references when it's pass-through.  Uses the ORIGINAL
     * property name ({@code relType}) on the emitted edge so collapsed and
     * non-collapsed graphs use the same edge labels.
     *
     * Cycle protection via {@code visited} (rdfIds already walked in this
     * chain) plus a hard {@code depth} cap.
     */
    private void collectEdges(String src,
                               String tgt,
                               String relType,
                               String neo4jVersion,
                               String orgId,
                               Set<String> includedCimTypes,
                               Map<String, PassThroughEntry> passThroughIndex,
                               Set<String> visited,
                               Map<String, List<Map<String, Object>>> byRelType,
                               int depth) {
        if (depth > MAX_COLLAPSE_DEPTH) return;
        if (!visited.add(tgt)) return;  // already walked → cycle, bail

        PassThroughEntry entry = passThroughIndex.get(tgt);
        // Unknown target (rare — the resolver should have filtered these out,
        // but defend against partial data): emit the edge anyway and let the
        // MATCH find-or-skip behaviour handle it.
        if (entry == null) {
            byRelType.computeIfAbsent(relType, k -> new ArrayList<>())
                    .add(buildRelRow(src, tgt, neo4jVersion, orgId));
            return;
        }

        // Target's cimType is exported → emit the edge directly.  This is the
        // base case — most edges hit it on the first hop.
        if (includedCimTypes.contains(entry.cimType)) {
            byRelType.computeIfAbsent(relType, k -> new ArrayList<>())
                    .add(buildRelRow(src, tgt, neo4jVersion, orgId));
            return;
        }

        // Pass-through: walk this target's own resolved refs.  Keep the
        // ORIGINAL relType so the final edge in the collapsed chain is labelled
        // by the property the source emitted (e.g. "Terminal.ConnectivityNode"
        // rather than "ACLineSegment.Terminals" — the legacy convention).
        // If the pass-through has no resolved refs, this is a dead end and
        // nothing is emitted.
        for (Map.Entry<String, List<String>> e : entry.resolved.entrySet()) {
            String nextRelType = "`" + e.getKey().replace("`", "") + "`";
            List<String> nextTargets = e.getValue();
            if (nextTargets == null) continue;
            for (String next : nextTargets) {
                if (next == null || next.isEmpty()) continue;
                collectEdges(src, next, nextRelType, neo4jVersion, orgId,
                        includedCimTypes, passThroughIndex, visited,
                        byRelType, depth + 1);
            }
        }
    }

    private Map<String, Object> buildRelRow(String src, String tgt,
                                              String neo4jVersion, String orgId) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("srcKey",  src + "|" + neo4jVersion + "|" + orgId);
        r.put("tgtKey",  tgt + "|" + neo4jVersion + "|" + orgId);
        r.put("version", neo4jVersion);
        r.put("orgId",   orgId);
        return r;
    }

    // ── Constraint creation (upfront, per label) ──────────────────────────

    private void createConstraintsUpfront(ExportRequest req) {
        if (constrainedLabels.size() > 0) return;
        log.info("Creating constraints and indexes...");
        long start = System.currentTimeMillis();

        // Create indexes on :CIMNode label upfront
        // cim_node_nodekey  → used by Pass 2 MATCH  (CRITICAL for performance)
        // cim_node_version  → used by version-filtered queries and clearExistingNodes
        // cim_node_orgid    → used by org-filtered queries
        String[][] sharedIndexes = {
            {"cim_node_nodekey",
             "CREATE INDEX cim_node_nodekey IF NOT EXISTS FOR (n:CIMNode) ON (n.nodeKey)"},
            {"cim_node_version",
             "CREATE INDEX cim_node_version IF NOT EXISTS FOR (n:CIMNode) ON (n.version)"},
            {"cim_node_orgid",
             "CREATE INDEX cim_node_orgid IF NOT EXISTS FOR (n:CIMNode) ON (n.orgId)"},
            {"cim_node_mrid",
             "CREATE INDEX cim_node_mrid IF NOT EXISTS FOR (n:CIMNode) ON (n.mrid)"},
        };
        for (String[] idx : sharedIndexes) {
            try (Session s = driver.session()) {
                s.run(idx[1]);
                log.debug("Index ensured: {}", idx[0]);
            } catch (Exception e) {
                log.debug("Index note {}: {}", idx[0], e.getMessage());
            }
        }
        log.info("CIMNode indexes ready — Pass 2 MATCH will use cim_node_nodekey");

        org.springframework.data.mongodb.core.aggregation.Aggregation agg =
            org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(
                org.springframework.data.mongodb.core.aggregation.Aggregation
                        .match(buildCriteria(req)),
                org.springframework.data.mongodb.core.aggregation.Aggregation
                        .group("cimType")
            );

        List<org.bson.Document> results = mongo.aggregate(
                agg, "cim_objects", org.bson.Document.class).getMappedResults();

        int created = 0;
        for (org.bson.Document doc : results) {
            Object ct = doc.get("_id");
            if (ct == null) continue;
            String label = sanitizeLabel(ct.toString());
            if (constrainedLabels.contains(label)) continue;
            try (Session s = driver.session()) {
                s.run("CREATE CONSTRAINT " + label + "_nodekey IF NOT EXISTS " +
                      "ON (n:" + label + ") ASSERT n.nodeKey IS UNIQUE");
                constrainedLabels.add(label);
                created++;
            } catch (Exception e) {
                constrainedLabels.add(label);
            }
        }
        log.info("Constraints ready: {} labels in {}",
                constrainedLabels.size(),
                com.cim.model.mongo.StageTimings.format(
                        System.currentTimeMillis() - start));
    }

    private void ensureLabelConstraint(Session s, String label) {
        if (constrainedLabels.contains(label)) return;
        try {
            s.run("CREATE CONSTRAINT " + label + "_nodekey IF NOT EXISTS " +
                  "ON (n:" + label + ") ASSERT n.nodeKey IS UNIQUE");
            constrainedLabels.add(label);
        } catch (Exception e) {
            constrainedLabels.add(label);
        }
    }

    // ── Clear existing nodes (batched, version+orgId scoped) ─────────────

    /**
     * Delete all nodes for a given version+orgId from Neo4j.
     *
     * THREE-STEP APPROACH (prevents transaction timeouts):
     *
     *   Step 1: Delete all RELATIONSHIPS first (batched).
     *           A Substation may have 50K+ relationships.
     *           DETACH DELETE 1000 nodes could mean deleting millions of rels
     *           in one transaction → TransactionDeadlockException / timeout.
     *           Deleting rels first makes DETACH DELETE safe (no rels left).
     *
     *   Step 2: Delete :CIMNode nodes (batch 5000 — fast, index-backed).
     *           All rels already gone → DETACH DELETE = simple node delete.
     *
     *   Step 3: Delete legacy nodes without :CIMNode label.
     *           Handles data exported before the :CIMNode fix was applied.
     *           Does NOT break on exception — logs and continues.
     *
     * DUPLICATE RISK if clear fails:
     *   CREATE mode (useBulkCreate=true): creates NEW nodes regardless.
     *     → duplicates if old nodes exist. Clear MUST succeed.
     *   MERGE mode (useBulkCreate=false): upserts existing nodes.
     *     → no duplicates regardless. Clear just cleans old extra nodes.
     */
    private long clearExistingNodesCount(String neo4jVersion, String orgId) {
        clearExistingNodes(neo4jVersion, orgId);
        return 0L; // count logged inside clearExistingNodes
    }

    private void clearExistingNodes(String neo4jVersion, String orgId) {
        log.info("Clearing nodes: version={} orgId={}", neo4jVersion, orgId);
        long relsDeleted = 0, nodesDeleted = 0;

        // ── Step 1: Delete relationships first ───────────────────────────
        // Batch: 10000 rels per transaction (relationships are lightweight)
        log.info("Step 1: deleting relationships for version={} orgId={}",
                neo4jVersion, orgId);
        // Step 1: Delete relationships
        // FIX: consume Result INSIDE writeTransaction lambda — not outside.
        // In Neo4j Driver 4.4.x, Result is only valid within the transaction.
        // Accessing r.single() after writeTransaction() returns = result already closed.
        while (true) {
            try (Session s = driver.session()) {
                long d = s.writeTransaction(tx -> {
                    Result r = tx.run(
                        "MATCH (n:CIMNode {version: $v, orgId: $o})-[rel]-() " +
                        "WITH rel LIMIT 10000 DELETE rel RETURN count(rel) AS d",
                        Values.parameters("v", neo4jVersion, "o", orgId));
                    return r.single().get("d").asLong(); // consumed inside tx ✅
                });
                relsDeleted += d;
                if (d == 0) break;
            } catch (Exception e) {
                log.warn("Rel delete batch error (retrying): {}", e.getMessage());
                try { Thread.sleep(500); } catch (InterruptedException ie) { break; }
            }
        }
        log.info("Relationships deleted: {}", relsDeleted);

        // Step 2: Delete :CIMNode nodes
        log.info("Step 2: deleting :CIMNode nodes for version={} orgId={}",
                neo4jVersion, orgId);
        int retries = 0;
        while (retries < 3) {
            boolean hadError = false;
            while (true) {
                try (Session s = driver.session()) {
                    long d = s.writeTransaction(tx -> {
                        Result r = tx.run(
                            "MATCH (n:CIMNode {version: $v, orgId: $o}) " +
                            "WITH n LIMIT 5000 DELETE n RETURN count(n) AS d",
                            Values.parameters("v", neo4jVersion, "o", orgId));
                        return r.single().get("d").asLong(); // consumed inside tx ✅
                    });
                    nodesDeleted += d;
                    if (d == 0) break;
                } catch (Exception e) {
                    log.warn("Node delete batch error: {}", e.getMessage());
                    hadError = true;
                    break;
                }
            }
            if (!hadError) break;
            retries++;
            try { Thread.sleep(1000); } catch (InterruptedException ie) { break; }
        }

        // Step 3: Delete legacy nodes without :CIMNode
        long legacyDeleted = 0;
        while (true) {
            try (Session s = driver.session()) {
                long d = s.writeTransaction(tx -> {
                    Result r = tx.run(
                        "MATCH (n {version: $v, orgId: $o}) WHERE NOT n:CIMNode " +
                        "WITH n LIMIT 2000 DETACH DELETE n RETURN count(n) AS d",
                        Values.parameters("v", neo4jVersion, "o", orgId));
                    return r.single().get("d").asLong(); // consumed inside tx ✅
                });
                legacyDeleted += d;
                nodesDeleted += d;
                if (d == 0) break;
            } catch (Exception e) {
                log.warn("Legacy node delete batch: {}", e.getMessage());
                break;
            }
        }

        log.info("Clear complete: {} relationships + {} nodes deleted (version={} orgId={})",
                relsDeleted, nodesDeleted, neo4jVersion, orgId);

        if (nodesDeleted == 0 && relsDeleted == 0)
            log.info("No existing data found for version={} orgId={} — fresh export",
                    neo4jVersion, orgId);
    }

    // ── Cursor pagination ─────────────────────────────────────────────────

    private List<CimObjectRecord> fetchPage(ExportRequest req, String lastId) {
        Criteria criteria = buildCriteria(req);
        if (lastId != null && !lastId.isBlank()) {
            criteria = new Criteria().andOperator(
                    criteria,
                    Criteria.where("_id").gt(
                            new org.bson.types.ObjectId(lastId)));
        }
        return mongo.find(
                new Query(criteria)
                        .limit(batchSize)
                        .with(Sort.by(Sort.Direction.ASC, "_id")),
                CimObjectRecord.class, "cim_objects");
    }

    private Criteria buildCriteria(ExportRequest req) {
        String jobId = req.getJobId();

        // CUSTOM with cimTypes list — used by all multi-variant exports
        if (req.getMode() == ExportMode.CUSTOM
                && req.getCimTypes() != null && !req.getCimTypes().isEmpty()) {
            return Criteria.where("jobId").is(jobId)
                    .and("cimType").in(req.getCimTypes());
        }

        if (req.getMode() == ExportMode.PHYSICAL_ONLY) {
            if (req.isIncludeNonCim()) {
                // PHYSICAL + vendor types (UNKNOWN/null category)
                return new Criteria().andOperator(
                        Criteria.where("jobId").is(jobId),
                        new Criteria().orOperator(
                                Criteria.where("category").is("PHYSICAL"),
                                Criteria.where("category").is("UNKNOWN"),
                                Criteria.where("category").isNull()
                        )
                );
            }
            return Criteria.where("jobId").is(jobId).and("category").is("PHYSICAL");
        }
        if (req.getMode() == ExportMode.REQUIRED_ONLY) {
            return Criteria.where("jobId").is(jobId).and("isRequired").is(true);
        }
        if (req.getMode() == ExportMode.TOPOLOGY) {
            List<String> logicalTypes = Arrays.asList(
                    "Terminal","ConnectivityNode","TopologicalNode","TopologicalIsland");
            List<Criteria> orList = new ArrayList<>();
            orList.add(Criteria.where("category").is("PHYSICAL"));
            orList.add(Criteria.where("cimType").in(logicalTypes));
            if (req.isIncludeNonCim()) {
                orList.add(Criteria.where("category").is("UNKNOWN"));
                orList.add(Criteria.where("category").isNull());
            }
            return new Criteria().andOperator(
                    Criteria.where("jobId").is(jobId),
                    new Criteria().orOperator(
                            orList.toArray(new Criteria[0]))
            );
        }
        // ALL
        return Criteria.where("jobId").is(jobId);
    }

    // ── Label sanitizer ───────────────────────────────────────────────────

    private String sanitizeLabel(String t) {
        if (t == null || t.isBlank()) return "Unknown";
        return t.replaceAll("[^A-Za-z0-9_]", "_");
    }

    // ── Cleanup legacy labels ─────────────────────────────────────────────

    public long removeLegacyLabels() {
        long total = 0;
        for (String label : Arrays.asList("CIMObject", "CIMNode")) {
            while (true) {
                try (Session s = driver.session()) {
                    String cypher = "MATCH (n:" + label + ") WITH n LIMIT 1000 " +
                                    "REMOVE n:" + label + " RETURN count(n) AS r";
                    long removed = s.writeTransaction(tx -> {
                        Result r = tx.run(cypher);
                        return r.single().get("r").asLong(); // consumed inside tx ✅
                    });
                    total += removed;
                    if (removed == 0) break;
                    log.info("Removed :{} from {} nodes", label, removed);
                } catch (Exception e) { break; }
            }
        }
        log.info("Legacy label cleanup: {} nodes updated", total);
        return total;
    }

    // ── Enhancement 4: OPEN state nodes for switches ─────────────────────

    /**
     * Creates a custom :OPEN node for every switch with normalOpen=true.
     *
     * WHY:
     *   In CIM, Switch.normalOpen=true means the switch is normally OPEN.
     *   This is a critical topological state — open switches break the
     *   electrical path in the network.
     *
     * WHAT IS CREATED:
     *   For each switch (Breaker/Fuse/Disconnector/Recloser) with normalOpen=true:
     *     - An :OPEN node representing the open state
     *     - A [:IS_NORMALLY_OPEN] relationship from the switch to the :OPEN node
     *
     *   Result in Neo4j:
     *     (breaker:Breaker {name:'BKR-01', normalOpen:'true'})
     *       -[:IS_NORMALLY_OPEN]→ (:OPEN {switchName:'BKR-01', version:'DB140-1'})
     *
     * SWITCH TYPES CHECKED: Breaker, Fuse, Disconnector, Recloser, Switch,
     *                        LoadBreakSwitch, GroundDisconnector, Jumper
     */
    private int createOpenStateNodes(String jobId, String neo4jVersion, String orgId) {
        // Switch cimTypes that have normalOpen attribute
        List<String> switchTypes = Arrays.asList(
            "Breaker", "Fuse", "Disconnector", "Recloser",
            "Switch", "LoadBreakSwitch", "GroundDisconnector", "Jumper"
        );

        // Find all switch objects with normalOpen=true in this job
        // normalOpen key is encoded as "Switch．normalOpen" in MongoDB
        String normalOpenKey = com.cim.util.MapKeySanitizer.encode("Switch.normalOpen");

        Criteria criteria = new Criteria().andOperator(
                Criteria.where("jobId").is(jobId),
                Criteria.where("cimType").in(switchTypes),
                new Criteria().orOperator(
                        // normalOpen stored under Switch.normalOpen key
                        Criteria.where("attributes." + normalOpenKey).is("true"),
                        // Some files store under cimType.normalOpen
                        Criteria.where("attributes." + com.cim.util.MapKeySanitizer.encode("Breaker.normalOpen")).is("true"),
                        Criteria.where("attributes." + com.cim.util.MapKeySanitizer.encode("Fuse.normalOpen")).is("true"),
                        Criteria.where("attributes." + com.cim.util.MapKeySanitizer.encode("Disconnector.normalOpen")).is("true"),
                        Criteria.where("attributes." + com.cim.util.MapKeySanitizer.encode("Recloser.normalOpen")).is("true")
                )
        );

        List<com.cim.model.mongo.CimObjectRecord> openSwitches =
                mongo.find(new Query(criteria), com.cim.model.mongo.CimObjectRecord.class,
                        "cim_objects");

        if (openSwitches.isEmpty()) return 0;

        log.info("Enhancement 4: found {} switches with normalOpen=true", openSwitches.size());

        // Batch create :OPEN nodes and relationships
        List<Map<String, Object>> openNodes = new ArrayList<>();
        for (com.cim.model.mongo.CimObjectRecord sw : openSwitches) {
            String nodeKey = sw.getRdfId() + "_OPEN|" + neo4jVersion + "|" + orgId;
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("nodeKey",    nodeKey);
            p.put("switchKey",  sw.getRdfId() + "|" + neo4jVersion + "|" + orgId);
            p.put("switchName", sw.getName() != null ? sw.getName() : "");
            p.put("switchType", sw.getCimType());
            p.put("switchRdfId",sw.getRdfId());
            p.put("version",    neo4jVersion);
            p.put("orgId",      orgId);
            p.put("jobId",      jobId);
            p.put("state",      "OPEN");
            openNodes.add(p);
        }

        // Write :OPEN nodes per switchType — group so each type gets correct labels
        // e.g. Breaker gets :OPEN:Breaker:CIMNode
        //      Disconnector gets :OPEN:Disconnector:CIMNode
        // This makes Neo4j Browser show different colors per type
        // and allows: MATCH (n:OPEN:Breaker) or MATCH (n:OPEN:Disconnector)

        // Group by switchType for label-specific CREATE
        Map<String, List<Map<String, Object>>> byType = new LinkedHashMap<>();
        for (Map<String, Object> node : openNodes) {
            String sType = (String) node.get("switchType");
            byType.computeIfAbsent(sType, k -> new ArrayList<>()).add(node);
        }

        int created = 0;
        try (Session s = driver.session()) {
            for (Map.Entry<String, List<Map<String, Object>>> e : byType.entrySet()) {
                String switchLabel = sanitizeLabel(e.getKey()); // e.g. "Breaker"
                List<Map<String, Object>> typeBatch = e.getValue();

                // :OPEN:{switchType}:CIMNode
                // e.g. :OPEN:Breaker:CIMNode  → shows as "OPEN Breaker" in Neo4j Browser
                String createCypher =
                    "UNWIND $batch AS p " +
                    "MERGE (o:OPEN:" + switchLabel + ":CIMNode {nodeKey: p.nodeKey}) " +
                    "SET o = p";
                final List<Map<String, Object>> fb = typeBatch;
                s.writeTransaction(tx -> {
                    tx.run(createCypher, Values.parameters("batch", fb));
                    return null;
                });
                created += typeBatch.size();
                log.debug("OPEN nodes created: type={} count={}", switchLabel, typeBatch.size());
            }

            // Create [:IS_NORMALLY_OPEN] relationships for all types
            String relCypher =
                "UNWIND $batch AS p " +
                "MATCH (sw:CIMNode {nodeKey: p.switchKey}) " +
                "MATCH (o:OPEN:CIMNode {nodeKey: p.nodeKey}) " +
                "MERGE (sw)-[:IS_NORMALLY_OPEN {version: p.version, orgId: p.orgId}]->(o)";
            final List<Map<String, Object>> allNodes = openNodes;
            s.writeTransaction(tx -> {
                tx.run(relCypher, Values.parameters("batch", allNodes));
                return null;
            });

        } catch (Exception e) {
            log.warn("OPEN node creation error: {}", e.getMessage());
        }

        return created;
    }

    // ── Migration — add :CIMNode to existing nodes ───────────────────────

    /**
     * Adds :CIMNode label to all existing nodes that don't have it yet.
     * Run this ONCE after deploying the :CIMNode fix.
     *
     * This is needed for:
     *   1. Pass 2 MATCH to find existing nodes via index
     *   2. clearExistingNodes() to delete them efficiently
     *   3. Version/orgId queries using CIMNode indexes
     *
     * Runs in batches of 5000 to avoid transaction timeouts.
     * Safe to run multiple times — idempotent (WHERE NOT n:CIMNode).
     */
    public long migrateAddCimNodeLabel() {
        log.info("Migration: adding :CIMNode label to existing nodes...");
        long total = 0;
        while (true) {
            try (Session s = driver.session()) {
                long updated = s.writeTransaction(tx -> {
                    Result r = tx.run(
                        "MATCH (n) WHERE NOT n:CIMNode AND n.nodeKey IS NOT NULL " +
                        "WITH n LIMIT 5000 SET n:CIMNode RETURN count(n) AS updated");
                    return r.single().get("updated").asLong(); // consumed inside tx ✅
                });
                total += updated;
                if (updated == 0) break;
                log.info("Migration progress: {} nodes labelled so far", total);
            } catch (Exception e) {
                log.warn("Migration batch error: {}", e.getMessage());
                break;
            }
        }
        log.info("Migration complete: {} nodes now have :CIMNode label", total);
        return total;
    }

    // ── Status ────────────────────────────────────────────────────────────

    public Optional<Neo4jExportJob> getExportStatus(String exportId) {
        return exportRepo.findByExportId(exportId);
    }

    public List<Neo4jExportJob> getRecentExports() {
        return exportRepo.findTop10ByOrderByStartedAtDesc();
    }
}
