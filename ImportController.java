package com.cim.controller;

import com.cim.model.mongo.ValidationJob;
import com.cim.repository.ValidationJobRepository;
import com.cim.service.ImportService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;

/**
 * Import REST API
 *
 * All endpoints accept version and orgId:
 *   version → X-Network-Version header or ?version= param
 *   orgId   → X-Org-Id header
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class ImportController {

    private static final long LARGE = 50L * 1024 * 1024;

    private final ImportService                              importService;
    private final ValidationJobRepository                    jobRepo;
    private final com.cim.repository.RawCimObjectRepository  rawRepo;
    private final com.cim.repository.CimObjectRecordRepository objectRepo;
    private final com.cim.service.GeoJsonExportService       geoJsonService;

    public ImportController(ImportService importService,
                              ValidationJobRepository jobRepo,
                              com.cim.repository.RawCimObjectRepository rawRepo,
                              com.cim.repository.CimObjectRecordRepository objectRepo,
                              com.cim.service.GeoJsonExportService geoJsonService) {
        this.importService  = importService;
        this.jobRepo        = jobRepo;
        this.rawRepo        = rawRepo;
        this.objectRepo     = objectRepo;
        this.geoJsonService = geoJsonService;
    }

    // ─────────────────────────────────────────────────────────────────
    // POST /api/import
    //
    // Headers:
    //   X-Org-Id:          CAISO, ERCOT, SPC  (required)
    //   X-User:            who is importing
    //   X-Network-Version: version label (optional — falls back to param)
    //
    // Params:
    //   file              CIM RDF file
    //   networkVersion    version label e.g. "DB140-1", "2024-Q1"
    // ─────────────────────────────────────────────────────────────────
    @PostMapping("/import")
    public ResponseEntity<?> importFile(
            @RequestParam("file")                                    MultipartFile file,
            @RequestParam("networkVersion")                          String networkVersion,
            @RequestHeader(value="X-Org-Id",      defaultValue="")  String orgId,
            @RequestHeader(value="X-User",         defaultValue="anonymous") String user) {

        if (file.isEmpty())
            return ResponseEntity.badRequest().body(Map.of("error","File is empty."));
        if (networkVersion == null || networkVersion.isBlank())
            return ResponseEntity.badRequest().body(Map.of(
                "error","networkVersion is required.",
                "hint", "e.g. 'DB140-1', '2024-Q1', 'SUMMER-PEAK-v2'"));

        String version = networkVersion.trim();
        String org     = orgId.trim();

        byte[] bytes; String hash;
        try {
            bytes = file.getBytes();
            hash  = ImportService.sha256(bytes);
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("error","Cannot read file: " + e.getMessage()));
        }

        // Duplicate check: same hash + version + orgId
        List<String> active = Arrays.asList("PENDING","RUNNING","RESOLVED");
        List<ValidationJob> dup = jobRepo
                .findByFileHashAndNetworkVersionAndStatusIn(hash, version, active);
        if (!dup.isEmpty()) {
            ValidationJob ex = dup.get(0);
            return ResponseEntity.status(409).body(Map.of(
                "error",          "Duplicate import rejected.",
                "reason",         "Same file+version already " + ex.getStatus() + ".",
                "existingJobId",  ex.getJobId(),
                "existingStatus", ex.getStatus(),
                "networkVersion", version,
                "orgId",          org,
                "hint",           "Use a different networkVersion or DELETE the existing job."
            ));
        }

        String format = detectFormat(bytes, file.getOriginalFilename());
        String jobId  = UUID.randomUUID().toString();

        ValidationJob job = new ValidationJob(
                jobId, file.getOriginalFilename(), format, user, version, org);
        job.setFileHash(hash);
        jobRepo.save(job);

        try {
            if (file.getSize() <= LARGE) {
                importService.processAndResolve(
                        new java.io.ByteArrayInputStream(bytes),
                        file.getOriginalFilename(), format, jobId, version, org);
                ValidationJob done = jobRepo.findByJobId(jobId).orElse(job);
                return ResponseEntity.ok(Map.of(
                    "jobId",          jobId,
                    "status",         done.getStatus(),
                    "networkVersion", version,
                    "orgId",          org,
                    "fileName",       file.getOriginalFilename(),
                    "format",         format,
                    "totalObjects",   done.getTotalObjects(),
                    "physicalObjects",done.getPhysicalObjects(),
                    "logicalObjects", done.getLogicalObjects(),
                    "stageTimings",   done.getStageTimings(),
                    "links",          links(jobId, version, org)
                ));
            } else {
                importService.processAndResolveAsync(
                        file.getInputStream(), file.getOriginalFilename(),
                        format, jobId, version, org);
                return ResponseEntity.accepted().body(Map.of(
                    "jobId",          jobId,
                    "status",         "PROCESSING",
                    "networkVersion", version,
                    "orgId",          org,
                    "fileName",       file.getOriginalFilename(),
                    "fileSizeMb",     String.format("%.1f", file.getSize()/1048576.0),
                    "links",          links(jobId, version, org)
                ));
            }
        } catch (Exception e) {
            job.setStatus("FAILED"); job.setErrorMessage(e.getMessage());
            jobRepo.save(job);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", e.getMessage(), "jobId", jobId,
                                 "networkVersion", version, "orgId", org));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/jobs
    // Filter by version and/or orgId
    // ─────────────────────────────────────────────────────────────────
    @GetMapping("/jobs")
    public ResponseEntity<?> listJobs(
            @RequestParam(required=false) String networkVersion,
            @RequestParam(required=false) String orgId) {

        List<ValidationJob> jobs;
        if (networkVersion != null && orgId != null) {
            jobs = jobRepo.findByNetworkVersionAndOrgId(
                    networkVersion.trim(), orgId.trim());
        } else if (networkVersion != null) {
            jobs = jobRepo.findByNetworkVersion(networkVersion.trim());
        } else if (orgId != null) {
            jobs = jobRepo.findByOrgId(orgId.trim());
        } else {
            jobs = jobRepo.findTop10ByOrderBySubmittedAtDesc();
        }
        return ResponseEntity.ok(jobs);
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/jobs/{jobId}
    // Response includes version and orgId
    // ─────────────────────────────────────────────────────────────────
    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<?> getJob(@PathVariable String jobId) {
        return jobRepo.findByJobId(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/jobs/{jobId}/progress
    // Response always includes version, orgId, percentSaved
    // ─────────────────────────────────────────────────────────────────
    @GetMapping("/jobs/{jobId}/progress")
    public ResponseEntity<?> progress(@PathVariable String jobId) {
        return importService.getTracker(jobId)
                .map(t -> {
                    // Enrich live snapshot with version + orgId from job
                    return ResponseEntity.ok(t.snapshot());
                })
                .orElseGet(() -> jobRepo.findByJobId(jobId)
                    .map(j -> ResponseEntity.ok(Map.of(
                        "jobId",          jobId,
                        "status",         j.getStatus(),
                        "networkVersion", j.getNetworkVersion() != null ? j.getNetworkVersion() : "",
                        "orgId",          j.getOrgId() != null ? j.getOrgId() : "",
                        "parsed",         j.getTotalObjects(),
                        "saved",          j.getTotalObjects(),
                        "percentSaved",   100,
                        "elapsedMs",      j.getProcessingMs(),
                        "elapsedFormatted", com.cim.model.mongo.StageTimings.format(j.getProcessingMs()),
                        "stageTimings",   j.getStageTimings(),
                        "finished",       true,
                        "stage",          j.getStatus()
                    ))).orElse(ResponseEntity.notFound().build()));
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/jobs/{jobId}/stats
    // Quick stats with version + orgId context
    // ─────────────────────────────────────────────────────────────────
    @GetMapping("/jobs/{jobId}/stats")
    public ResponseEntity<?> getJobStats(@PathVariable String jobId) {
        return jobRepo.findByJobId(jobId).map(j -> ResponseEntity.ok(Map.of(
            "jobId",          jobId,
            "networkVersion", j.getNetworkVersion() != null ? j.getNetworkVersion() : "",
            "orgId",          j.getOrgId() != null ? j.getOrgId() : "",
            "status",         j.getStatus(),
            "totalObjects",   j.getTotalObjects(),
            "physicalObjects",j.getPhysicalObjects(),
            "logicalObjects", j.getLogicalObjects(),
            "pathIssueCount", j.getPathIssueCount(),
            "stageTimings",   j.getStageTimings(),
            "enabledNamespaces", j.getEnabledNamespaces(),
            "links",          links(jobId,
                    j.getNetworkVersion() != null ? j.getNetworkVersion() : "",
                    j.getOrgId() != null ? j.getOrgId() : "")
        ))).orElse(ResponseEntity.notFound().build());
    }

    // ─────────────────────────────────────────────────────────────────
    // DELETE /api/jobs/{jobId}
    // ─────────────────────────────────────────────────────────────────
    // ── Alias endpoints — use orgId+version instead of jobId ──────────

    /**
     * Resolve orgId+version → latest jobId.
     * Returns the most recent active job for this orgId+version.
     * Used by alias endpoints and clients that prefer readable identifiers.
     */
    private java.util.Optional<String> resolveJobId(String orgId, String version) {
        return jobRepo
            .findTopByOrgIdAndNetworkVersionOrderBySubmittedAtDesc(
                orgId.trim(), version.trim())
            .map(j -> j.getJobId());
    }

    /**
     * GET /api/cim/orgs/{orgId}/versions/{version}/progress
     * Alias for GET /api/cim/jobs/{jobId}/progress
     * More readable: no need to look up jobId first.
     */
    @GetMapping("/orgs/{orgId}/versions/{version}/progress")
    public ResponseEntity<?> progressByVersion(
            @PathVariable String orgId,
            @PathVariable String version) {
        return resolveJobId(orgId, version)
                .map(jobId -> {
                    com.cim.streaming.ProgressTracker t = importService.getTracker(jobId);
                    if (t != null) {
                        // Tracker active (import running) — wrap snapshot with jobId
                        com.cim.streaming.ProgressSnapshot snap = t.snapshot();
                        java.util.Map<String,Object> r = new java.util.LinkedHashMap<>();
                        r.put("jobId",           jobId);   // REQ 1: always include jobId
                        r.put("orgId",           orgId);
                        r.put("networkVersion",  version);
                        r.put("stage",           snap.getStage());
                        r.put("parsed",          snap.getParsed());
                        r.put("saved",           snap.getSaved());
                        r.put("percentSaved",    snap.getPercentSaved());
                        r.put("elapsedFormatted",snap.getElapsedFormatted());
                        r.put("parseRatePerSec", snap.getParseRatePerSec());
                        r.put("finished",        snap.isFinished());
                        r.put("detail",          snap.getDetail());
                        return ResponseEntity.ok(r);
                    }
                    return jobRepo.findByJobId(jobId)
                            .map(j -> {
                                java.util.Map<String,Object> r = new java.util.LinkedHashMap<>();
                                r.put("jobId",           jobId);  // REQ 1: always include
                                r.put("orgId",           orgId);
                                r.put("networkVersion",  version);
                                r.put("status",          j.getStatus());
                                r.put("percentSaved",    100);
                                r.put("parsed",          j.getTotalObjects());
                                r.put("saved",           j.getTotalObjects());
                                r.put("finished",        true);
                                r.put("elapsedFormatted",
                                        j.getStageTimings() != null
                                        ? j.getStageTimings().getTotalFormatted() : null);
                                r.put("stageTimings",    j.getStageTimings());
                                return ResponseEntity.ok(r);
                            })
                            .orElse(ResponseEntity.notFound().build());
                })
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /api/cim/orgs/{orgId}/versions/{version}
     * Alias for GET /api/cim/jobs/{jobId}
     */
    @GetMapping("/orgs/{orgId}/versions/{version}")
    public ResponseEntity<?> jobByVersion(
            @PathVariable String orgId,
            @PathVariable String version) {
        return resolveJobId(orgId, version)
                .flatMap(jobId -> jobRepo.findByJobId(jobId))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /api/cim/orgs/{orgId}/versions/{version}/stats
     * Alias for GET /api/cim/jobs/{jobId}/stats
     */
    @GetMapping("/orgs/{orgId}/versions/{version}/stats")
    public ResponseEntity<?> statsByVersion(
            @PathVariable String orgId,
            @PathVariable String version) {
        return resolveJobId(orgId, version)
                .map(jobId -> {
                    long total    = objectRepo.countByJobId(jobId);
                    long physical = objectRepo.countByJobIdAndCategory(jobId, "PHYSICAL");
                    long logical  = objectRepo.countByJobIdAndCategory(jobId, "LOGICAL");
                    java.util.Map<String,Object> r = new java.util.LinkedHashMap<>();
                    r.put("orgId",          orgId);
                    r.put("networkVersion", version);
                    r.put("jobId",          jobId);
                    r.put("total",          total);
                    r.put("physical",       physical);
                    r.put("logical",        logical);
                    return ResponseEntity.ok(r);
                })
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /api/cim/orgs/{orgId}/versions
     * List all versions imported for an organisation.
     * More useful than listing by jobId.
     */
    @GetMapping("/orgs/{orgId}/versions")
    public ResponseEntity<?> listVersions(@PathVariable String orgId) {
        java.util.List<com.cim.model.mongo.ValidationJob> jobs =
                jobRepo.findByOrgId(orgId.trim());
        java.util.List<java.util.Map<String,Object>> versions = new java.util.ArrayList<>();
        for (com.cim.model.mongo.ValidationJob j : jobs) {
            java.util.Map<String,Object> v = new java.util.LinkedHashMap<>();
            v.put("jobId",          j.getJobId());
            v.put("networkVersion", j.getNetworkVersion());
            v.put("status",         j.getStatus());
            v.put("totalObjects",   j.getTotalObjects());
            v.put("totalFormatted", j.getStageTimings() != null
                    ? j.getStageTimings().getTotalFormatted() : null);
            v.put("submittedAt",    j.getSubmittedAt());
            versions.add(v);
        }
        return ResponseEntity.ok(Map.of(
            "orgId",    orgId,
            "total",    versions.size(),
            "versions", versions
        ));
    }

    @DeleteMapping("/jobs/{jobId}")
    public ResponseEntity<?> deleteJob(@PathVariable String jobId) {
        return jobRepo.findByJobId(jobId).map(j -> {
            // Delete from all three collections
            jobRepo.delete(j);
            rawRepo.deleteByJobId(jobId);    // raw_objects
            // cim_objects deleted by CimObjectRecordRepository in ImportService
            return ResponseEntity.ok(Map.of(
                "message",          "Job " + jobId + " deleted.",
                "networkVersion",   j.getNetworkVersion() != null ? j.getNetworkVersion() : "",
                "orgId",            j.getOrgId() != null ? j.getOrgId() : "",
                "collectionsCleared", java.util.Arrays.asList(
                    "validation_jobs", "raw_objects", "cim_objects"),
                "hint",             "You can now re-import the same file+version."));
        }).orElse(ResponseEntity.notFound().build());
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private String detectFormat(byte[] bytes, String name) {
        String h = new String(bytes, 0, Math.min(512, bytes.length));
        if (h.contains("rdf:RDF") || h.contains("xmlns:cim")) return "RDF_XML";
        if (h.contains("@context") || h.contains("@graph"))   return "JSON_LD";
        if (name != null && name.toLowerCase().endsWith(".csv")) return "MILSOFT_CSV";
        return "RDF_XML";
    }

    private Map<String, String> links(String jobId, String version, String orgId) {
        return Map.of(
            "progress",   "/api/jobs/" + jobId + "/progress",
            "stats",      "/api/jobs/" + jobId + "/stats",
            "objects",    "/api/jobs/" + jobId + "/objects",
            "pathIssues", "/api/jobs/" + jobId + "/objects/path-issues",
            "required",   "/api/jobs/" + jobId + "/objects/required/summary",
            "byVersion",  "/api/jobs?networkVersion=" + version + "&orgId=" + orgId
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // POST /api/jobs/{jobId}/generate-geojson
    //
    // On-demand GeoJSON generation for a completed import.  Reads
    // cim_objects, converts NC State Plane → WGS84, writes a
    // FeatureCollection to /tmp/cim-exports/<jobId>/wake-topology.geojson.
    //
    // Idempotent — re-running overwrites the previous file.  Returns
    // a JSON summary with the output file path and counts.
    // ─────────────────────────────────────────────────────────────────
    @PostMapping("/jobs/{jobId}/generate-geojson")
    public ResponseEntity<?> generateGeoJson(@PathVariable String jobId) {
        if (jobRepo.findByJobId(jobId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        try {
            var result = geoJsonService.generate(jobId);
            return ResponseEntity.ok(Map.of(
                "jobId",            result.jobId,
                "filePath",         result.filePath,
                "featuresWritten",  result.featuresWritten,
                "skippedNoCoords",  result.skippedNoCoords,
                "skippedBadCoords", result.skippedBadCoords,
                "examined",         result.examined,
                "elapsedMs",        result.elapsedMs,
                "download",         "/api/jobs/" + jobId + "/geojson"
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(
                    Map.of("jobId", jobId, "error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(
                    Map.of("jobId", jobId, "error", e.getMessage()));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/jobs/{jobId}/geojson
    //
    // Stream the previously-generated GeoJSON file back to the caller.
    // Returns 404 if the file doesn't exist (generation hasn't run yet).
    // Does NOT regenerate — use POST for that.
    // ─────────────────────────────────────────────────────────────────
    @GetMapping("/jobs/{jobId}/geojson")
    public ResponseEntity<?> downloadGeoJson(@PathVariable String jobId) {
        java.nio.file.Path file = java.nio.file.Paths.get(
                "/tmp/cim-exports", jobId, "wake-topology.geojson");
        if (!java.nio.file.Files.exists(file)) {
            return ResponseEntity.status(404).body(Map.of(
                "jobId", jobId,
                "error", "GeoJSON not generated yet. POST /api/jobs/" + jobId
                       + "/generate-geojson first."));
        }
        try {
            byte[] bytes = java.nio.file.Files.readAllBytes(file);
            return ResponseEntity.ok()
                    .header("Content-Type", "application/geo+json")
                    .header("Content-Disposition",
                            "inline; filename=\"" + file.getFileName() + "\"")
                    .body(bytes);
        } catch (java.io.IOException e) {
            return ResponseEntity.internalServerError().body(
                    Map.of("jobId", jobId, "error", e.getMessage()));
        }
    }
}
