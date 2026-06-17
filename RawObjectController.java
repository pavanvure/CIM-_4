package com.cim.controller;

import com.cim.model.mongo.RawCimObject;
import com.cim.repository.RawCimObjectRepository;
import com.cim.util.MapKeySanitizer;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API for raw_objects collection.
 *
 * raw_objects contains everything parsed directly from the file —
 * no namespace filtering, no path validation, no empty-value removal.
 * Use it for:
 *   - Auditing what was in the original file
 *   - Comparing raw vs validated (cim_objects) for the same rdfId
 *   - Re-processing with different namespace/validation rules
 *   - Debugging path issues (see what was actually in the file)
 *
 * Endpoints:
 *   GET /api/cim/jobs/{jobId}/raw              List raw objects (paginated)
 *   GET /api/cim/jobs/{jobId}/raw/stats        Count summary
 *   GET /api/cim/raw/{id}                      Single raw object by _id
 *   GET /api/cim/jobs/{jobId}/raw/compare/{rdfId}  Compare raw vs cim for same object
 */
@RestController
@RequestMapping("/api/cim")
@CrossOrigin(origins = "*")
public class RawObjectController {

    private final RawCimObjectRepository rawRepo;
    private final MongoTemplate          mongo;

    public RawObjectController(RawCimObjectRepository rawRepo,
                                MongoTemplate mongo) {
        this.rawRepo = rawRepo;
        this.mongo   = mongo;
    }

    /**
     * GET /api/cim/jobs/{jobId}/raw
     * List raw objects for a job. Filter by cimType, page, size.
     */
    @GetMapping("/jobs/{jobId}/raw")
    public ResponseEntity<?> listRaw(
            @PathVariable String jobId,
            @RequestParam(required=false) String type,
            @RequestParam(defaultValue="0")   int page,
            @RequestParam(defaultValue="50")  int size) {

        Criteria criteria = Criteria.where("jobId").is(jobId);
        if (type != null && !type.isBlank())
            criteria = criteria.and("cimType").is(type.trim());

        Query q = new Query(criteria).with(PageRequest.of(page, size));
        long total = mongo.count(new Query(criteria), "raw_objects");
        java.util.List<RawCimObject> items = mongo.find(q, RawCimObject.class, "raw_objects");

        return ResponseEntity.ok(Map.of(
            "jobId",         jobId,
            "total",         total,
            "page",          page,
            "size",          size,
            "totalPages",    (int) Math.ceil((double) total / size),
            "content",       items
        ));
    }

    /**
     * GET /api/cim/jobs/{jobId}/raw/stats
     * Count of raw objects by cimType.
     */
    @GetMapping("/jobs/{jobId}/raw/stats")
    public ResponseEntity<?> rawStats(@PathVariable String jobId) {
        long total = rawRepo.countByJobId(jobId);

        org.springframework.data.mongodb.core.aggregation.Aggregation agg =
            org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(
                org.springframework.data.mongodb.core.aggregation.Aggregation
                        .match(Criteria.where("jobId").is(jobId)),
                org.springframework.data.mongodb.core.aggregation.Aggregation
                        .group("cimType").count().as("count"),
                org.springframework.data.mongodb.core.aggregation.Aggregation
                        .sort(org.springframework.data.domain.Sort.Direction.DESC, "count")
            );

        java.util.List<org.bson.Document> byType = mongo.aggregate(
                agg, "raw_objects", org.bson.Document.class).getMappedResults();

        return ResponseEntity.ok(Map.of(
            "jobId",   jobId,
            "total",   total,
            "byType",  byType
        ));
    }

    /**
     * GET /api/cim/raw/{id}
     * Single raw object by MongoDB _id.
     * Returns decoded attribute keys (U+FF0E → dot) for readability.
     */
    @GetMapping("/raw/{id}")
    public ResponseEntity<?> getOne(@PathVariable String id) {
        return rawRepo.findById(id).map(raw -> {
            // Decode keys for display: U+FF0E → dot
            java.util.Map<String, Object> resp = new java.util.LinkedHashMap<>();
            resp.put("id",           raw.getId());
            resp.put("jobId",        raw.getJobId());
            resp.put("rdfId",        raw.getRdfId());
            resp.put("mrid",         raw.getMrid());
            resp.put("cimType",      raw.getCimType());
            resp.put("name",         raw.getName());
            resp.put("orgId",        raw.getOrgId());
            resp.put("version",      raw.getVersion());
            resp.put("sourceFormat", raw.getSourceFormat());
            resp.put("sourceFile",   raw.getSourceFile());
            // Decode attribute keys for human readability
            resp.put("rawAttributes",
                     MapKeySanitizer.decodeKeys(raw.getRawAttributes()));
            resp.put("rawReferences",
                     MapKeySanitizer.decodeKeysList(raw.getRawReferences()));
            resp.put("createdAt",    raw.getCreatedAt());
            return ResponseEntity.ok(resp);
        }).orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /api/cim/jobs/{jobId}/raw/compare/{rdfId}
     *
     * Side-by-side comparison of raw vs CIM-validated object for the same rdfId.
     * Useful for:
     *   - Understanding what path validation filtered out
     *   - Seeing which namespace attributes were excluded
     *   - Debugging missing attributes in cim_objects
     */
    @GetMapping("/jobs/{jobId}/raw/compare/{rdfId}")
    public ResponseEntity<?> compare(
            @PathVariable String jobId,
            @PathVariable String rdfId) {

        // Get raw object
        Query rawQuery = new Query(
                Criteria.where("jobId").is(jobId).and("rdfId").is(rdfId));
        RawCimObject raw = mongo.findOne(rawQuery, RawCimObject.class, "raw_objects");

        // Get validated CIM object
        com.cim.model.mongo.CimObjectRecord cim = mongo.findOne(
                rawQuery, com.cim.model.mongo.CimObjectRecord.class, "cim_objects");

        if (raw == null && cim == null)
            return ResponseEntity.notFound().build();

        java.util.Map<String, Object> resp = new java.util.LinkedHashMap<>();
        resp.put("jobId", jobId);
        resp.put("rdfId", rdfId);

        // Raw side
        if (raw != null) {
            java.util.Map<String, Object> rawSide = new java.util.LinkedHashMap<>();
            rawSide.put("cimType",         raw.getCimType());
            rawSide.put("name",            raw.getName());
            rawSide.put("totalAttributes", raw.getRawAttributes().size());
            rawSide.put("totalReferences", raw.getRawReferences().size());
            rawSide.put("attributes",      MapKeySanitizer.decodeKeys(raw.getRawAttributes()));
            rawSide.put("references",      MapKeySanitizer.decodeKeysList(raw.getRawReferences()));
            resp.put("raw_objects", rawSide);
        } else {
            resp.put("raw_objects", "NOT FOUND");
        }

        // CIM validated side
        if (cim != null) {
            java.util.Map<String, Object> cimSide = new java.util.LinkedHashMap<>();
            cimSide.put("cimType",         cim.getCimType());
            cimSide.put("name",            cim.getName());
            cimSide.put("category",        cim.getCategory());
            cimSide.put("isRequired",      cim.isRequired());
            cimSide.put("totalAttributes", cim.getAttributes() != null ? cim.getAttributes().size() : 0);
            cimSide.put("totalReferences", cim.getReferences() != null ? cim.getReferences().size() : 0);
            cimSide.put("pathIssueCount",  cim.getPathIssues() != null ? cim.getPathIssues().size() : 0);
            cimSide.put("attributes",
                    cim.getAttributes() != null
                    ? MapKeySanitizer.decodeKeys(cim.getAttributes())
                    : java.util.Collections.emptyMap());
            cimSide.put("resolvedRefIds",
                    cim.getResolvedRefIds() != null
                    ? MapKeySanitizer.decodeKeysList(cim.getResolvedRefIds())
                    : java.util.Collections.emptyMap());
            cimSide.put("pathIssues",  cim.getPathIssues());
            resp.put("cim_objects", cimSide);
        } else {
            resp.put("cim_objects", "NOT FOUND");
        }

        // Diff summary — what was filtered out
        if (raw != null && cim != null) {
            java.util.Map<String, String> rawAttrs = MapKeySanitizer.decodeKeys(raw.getRawAttributes());
            java.util.Map<String, String> cimAttrs = MapKeySanitizer.decodeKeys(
                    cim.getAttributes() != null ? cim.getAttributes() : new java.util.LinkedHashMap<>());

            java.util.List<String> filteredOut = new java.util.ArrayList<>();
            for (String key : rawAttrs.keySet()) {
                if (!cimAttrs.containsKey(key)) filteredOut.add(key);
            }

            resp.put("diff", Map.of(
                "rawAttributeCount", rawAttrs.size(),
                "cimAttributeCount", cimAttrs.size(),
                "filteredOutCount",  filteredOut.size(),
                "filteredOutKeys",   filteredOut,
                "explanation",       "Filtered keys are either from disabled namespaces "
                                   + "or had blank/null values in the file."
            ));
        }

        return ResponseEntity.ok(resp);
    }
}
