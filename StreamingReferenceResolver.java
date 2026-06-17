package com.cim.streaming;

import com.cim.model.mongo.CimObjectRecord;
import com.cim.repository.CimObjectRecordRepository;
import com.cim.util.MapKeySanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import org.bson.Document;

import java.util.*;

@Component
public class StreamingReferenceResolver {

    private static final Logger log =
            LoggerFactory.getLogger(StreamingReferenceResolver.class);
    private static final int BATCH = 500;

    private final CimObjectRecordRepository repo;
    private final MongoTemplate             mongo;

    public StreamingReferenceResolver(CimObjectRecordRepository repo,
                                       MongoTemplate mongo) {
        this.repo  = repo;
        this.mongo = mongo;
    }

    public long resolveReferencesInMongo(String jobId, int batchSize,
                                          ProgressTracker progress) {
        progress.log("Reference resolution: server-side $lookup+$merge");
        long start = System.currentTimeMillis();
        try {
            long n = serverSide(jobId, progress, start);
            progress.log("Server-side done in " + elapsed(start) + "s → " + n + " resolved");
            return n;
        } catch (Exception e) {
            log.warn("Server-side failed ({}), client-side fallback", e.getMessage());
            return clientSide(jobId, batchSize, progress, start);
        }
    }

    // ── Server-side $lookup + $merge ──────────────────────────────────────
    //
    // References are now Map<String, List<String>>.  After $objectToArray we
    // get an array of {k: propName, v: [target1, target2, ...]} entries.  For
    // each entry, we need to look up each element of v against cim_objects.rdfId
    // (within the same job), keep only those that resolved, and emit the
    // filtered list back into resolvedRefIds with the original key.
    //
    // Strategy: $unwind refArray so each entry is its own document, $unwind
    // entry.v to get one row per (k, target) pair, $lookup each target,
    // filter, then re-group back to {k: [resolved targets]}.  This is more
    // verbose than the single-value version but maps cleanly to the new shape.
    private long serverSide(String jobId, ProgressTracker progress, long start) {
        String pipeline = String.format(
            "{ aggregate: 'cim_objects', pipeline: ["
          + "  { $match: { jobId: '%s' } },"
          + "  { $project: { _id:1, refArray: { $objectToArray: '$references' } } },"
          + "  { $unwind: { path:'$refArray', preserveNullAndEmptyArrays:false } },"
          + "  { $unwind: { path:'$refArray.v', preserveNullAndEmptyArrays:false } },"
          + "  { $lookup: { from:'cim_objects', let:{ tgt:'$refArray.v' },"
          + "    pipeline:[{ $match: { $expr: { $and:["
          + "      { $eq:['$rdfId','$$tgt'] },"
          + "      { $eq:['$jobId','%s'] }"
          + "    ]}}},{ $project:{ _id:0, rdfId:1 } },{ $limit:1 }], as:'hit' } },"
          + "  { $match: { hit: { $ne: [] } } },"
          + "  { $group: { _id: { docId:'$_id', key:'$refArray.k' },"
          + "             targets: { $push:'$refArray.v' } } },"
          + "  { $group: { _id:'$_id.docId',"
          + "             pairs: { $push: { k:'$_id.key', v:'$targets' } } } },"
          + "  { $addFields: { resolvedRefIds: { $arrayToObject:'$pairs' } } },"
          + "  { $project: { resolvedRefIds:1 } },"
          + "  { $merge: { into:'cim_objects', on:'_id',"
          + "    whenMatched:'merge', whenNotMatched:'discard' } }"
          + "], cursor:{}, allowDiskUse:true}", jobId, jobId);

        mongo.getDb().runCommand(Document.parse(pipeline));
        progress.log("$merge done in " + elapsed(start) + "s");

        Criteria c = Criteria.where("resolvedRefIds").exists(true).ne(new Document());
        return mongo.count(new Query(Criteria.where("jobId").is(jobId).andOperator(c)), "cim_objects");
    }

    // ── Client-side fallback ──────────────────────────────────────────────
    private long clientSide(String jobId, int batchSize,
                             ProgressTracker progress, long start) {
        Set<String> targets  = collectTargets(jobId, progress);
        progress.log("Targets collected: " + targets.size() + " in " + elapsed(start) + "s");
        Set<String> existing = verifyExistence(jobId, targets);
        progress.log("Verified: " + existing.size() + "/" + targets.size());
        return writeBack(jobId, existing, batchSize, progress);
    }

    private Set<String> collectTargets(String jobId, ProgressTracker progress) {
        try {
            // References are Map<String, List<String>> → after $objectToArray
            // each entry has {k: prop, v: [targets]}.  Unwind both the entries
            // and their value arrays so each row is one (k, target) pair, then
            // collect distinct target ids.
            String pipe = String.format(
                "{ aggregate:'cim_objects', pipeline:["
              + "  { $match:{ jobId:'%s', references:{ $exists:true } } },"
              + "  { $project:{ _id:0, refs:{ $objectToArray:'$references' } } },"
              + "  { $unwind:'$refs' },"
              + "  { $unwind:'$refs.v' },"
              + "  { $group:{ _id:'$refs.v' } }"
              + "], cursor:{batchSize:50000}, allowDiskUse:true}", jobId);
            Document res = mongo.getDb().runCommand(Document.parse(pipe));
            Set<String> targets = new HashSet<>();
            Document cursor = (Document) res.get("cursor");
            if (cursor != null) {
                @SuppressWarnings("unchecked")
                List<Document> batch = (List<Document>) cursor.get("firstBatch");
                if (batch != null) batch.forEach(d -> {
                    Object id = d.get("_id");
                    if (id != null && !id.toString().isBlank()) targets.add(id.toString());
                });
            }
            return targets;
        } catch (Exception e) { return new HashSet<>(); }
    }

    private Set<String> verifyExistence(String jobId, Set<String> targets) {
        Set<String> found = new HashSet<>();
        List<String> list = new ArrayList<>(targets);
        for (int i = 0; i < list.size(); i += BATCH) {
            List<String> b = list.subList(i, Math.min(i+BATCH, list.size()));
            Query q = new Query(Criteria.where("jobId").is(jobId).and("rdfId").in(b));
            q.fields().include("rdfId").exclude("_id");
            q.withHint(Document.parse("{jobId:1,rdfId:1}"));
            mongo.find(q, CimObjectRecord.class, "cim_objects")
                 .forEach(r -> { if (r.getRdfId() != null) found.add(r.getRdfId()); });
        }
        return found;
    }

    private long writeBack(String jobId, Set<String> existing,
                            int batchSize, ProgressTracker progress) {
        long total = 0; int page = 0;
        while (true) {
            List<CimObjectRecord> batch = repo.findByJobId(jobId,
                    PageRequest.of(page, batchSize)).getContent();
            if (batch.isEmpty()) break;
            BulkOperations bulk = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, "cim_objects");
            int upd = 0;
            for (CimObjectRecord rec : batch) {
                // References are now Map<String, List<String>> — each key can
                // hold multiple targets (e.g. ACLineSegment.Terminals).  For
                // each key, keep the subset of targets that resolved to an
                // existing rdfId in this job.  Empty result lists are dropped
                // so resolvedRefIds only contains keys with at least one
                // successful resolution.
                Map<String, List<String>> refs = rec.getReferences();
                if (refs == null || refs.isEmpty()) continue;
                Map<String, List<String>> resolved = new LinkedHashMap<>();
                MapKeySanitizer.decodeKeysList(refs).forEach((k, targets) -> {
                    if (targets == null || targets.isEmpty()) return;
                    List<String> kept = new ArrayList<>();
                    for (String t : targets) {
                        if (existing.contains(t)) kept.add(t);
                    }
                    if (!kept.isEmpty()) {
                        resolved.put(MapKeySanitizer.encode(k), kept);
                    }
                });
                if (!resolved.isEmpty()) {
                    bulk.updateOne(new Query(Criteria.where("_id").is(rec.getId())),
                            new Update().set("resolvedRefIds", resolved));
                    // Count total individual (key, target) resolutions.
                    int n = 0;
                    for (List<String> lst : resolved.values()) n += lst.size();
                    total += n; upd++;
                }
            }
            if (upd > 0) bulk.execute();
            page++;
        }
        return total;
    }

    private long elapsed(long start) { return (System.currentTimeMillis()-start)/1000; }
}
