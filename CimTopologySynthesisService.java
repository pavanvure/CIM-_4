package com.cim.service;

import com.cim.model.mongo.CimObjectRecord;
import com.cim.repository.CimObjectRecordRepository;
import com.cim.repository.ValidationJobRepository;
import com.cim.util.MapKeySanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Second-pass CIM topology synthesis for Milsoft-imported data.
 *
 * <h2>Why this exists</h2>
 * Milsoft `.STD` files express connectivity as a flat parent-pointer tree
 * (each row's column 4 names its upstream section). CIM expresses
 * connectivity through {@code Terminal} → {@code ConnectivityNode} →
 * {@code Terminal} edges. The two are not interchangeable: tools and
 * queries written against the CIM connectivity model can't traverse the
 * Milsoft tree without bespoke knowledge.
 *
 * <p>This service reads the already-parsed {@code cim_objects} for a job
 * and emits the missing CIM structural objects:
 * <ul>
 *   <li>One {@code Terminal} per port on each equipment object.</li>
 *   <li>One {@code ConnectivityNode} per electrical junction in the tree,
 *       shared by all equipment whose terminals meet there.</li>
 *   <li>Proper {@code Terminal.ConductingEquipment} and
 *       {@code Terminal.ConnectivityNode} references on each new terminal.</li>
 *   <li>{@code IdentifiedObject.mRID} populated on every object (existing
 *       and synthesized) that lacked one — using deterministic UUIDs so
 *       re-running the synthesis on the same data yields identical UUIDs.</li>
 * </ul>
 *
 * <h2>Determinism</h2>
 * UUIDs are derived via {@link UUID#nameUUIDFromBytes(byte[])} (Java's
 * MD5-based name UUID, RFC 4122 §4.3 / UUIDv3 in spirit; functionally
 * equivalent to UUIDv5 for our purposes — same input always yields the
 * same UUID). Seed strings:
 * <pre>
 *   equipment   "milsoft:equipment:" + rdfId
 *   source term "milsoft:term:source:" + rdfId
 *   load term   "milsoft:term:load:" + rdfId
 *   single term "milsoft:term:" + rdfId      (for one-port devices)
 *   conn node   "milsoft:cn:" + parentRdfId  (shared by all siblings)
 *   root cn     "milsoft:cn:ROOT:" + rootRdfId (one per root)
 *   leaf cn     "milsoft:cn:leaf:" + leafRdfId (one per dead-end load side)
 * </pre>
 * The ConnectivityNode-sharing property falls out of the seed scheme
 * automatically: two siblings under parent {@code BU} both compute
 * {@code "milsoft:cn:BU"}, both get the same UUID, both point at the same
 * synthesized node.
 *
 * <h2>What this does NOT do</h2>
 * <ul>
 *   <li>It does not touch {@code raw_objects} — the audit trail is unchanged.</li>
 *   <li>It does not modify the parser; synthesis is purely additive.</li>
 *   <li>It does not refine switch subtypes — existing code-10 rows remain
 *       {@code ProtectedSwitch}.</li>
 *   <li>It does not split {@code PowerTransformer} into per-winding ends.</li>
 * </ul>
 *
 * <h2>Idempotence</h2>
 * Running synthesize twice is safe because deterministic UUIDs cause the
 * second run to recompute identical synthesized objects. We delete prior
 * synthesized rows (where {@code synthesized=true}) at the start of each
 * run to keep insert paths simple. Re-synthesis after parsed objects
 * change (e.g. revalidate) regenerates the topology fresh.
 */
@Service
public class CimTopologySynthesisService {

    private static final Logger log =
            LoggerFactory.getLogger(CimTopologySynthesisService.class);

    /**
     * Equipment classes that get TWO terminals (source + load).
     * Anything with through-current — lines, switches, transformers.
     */
    private static final Set<String> TWO_PORT = new HashSet<>(Arrays.asList(
            "ACLineSegment",
            "PowerTransformer",
            "RatioTapChanger",
            "Switch",
            "LoadBreakSwitch",
            "ProtectedSwitch",
            "Breaker",
            "Disconnector",
            "Fuse",
            "Recloser",
            "Sectionaliser",
            "GroundDisconnector",
            "Jumper"
    ));

    /**
     * Equipment classes that get ONE terminal (end devices).
     * Sources, loads, capacitors, machines, service points.
     */
    private static final Set<String> ONE_PORT = new HashSet<>(Arrays.asList(
            "EnergySource",
            "EnergyConsumer",
            "LinearShuntCompensator",
            "NonlinearShuntCompensator",
            "UsagePoint",
            "AsynchronousMachine",
            "SynchronousMachine",
            "BusbarSection"
    ));

    /**
     * Classes that get NO synthetic terminals.
     * ConnectivityNode IS the connection point; Substation/Feeder are
     * containers, not equipment.
     */
    private static final Set<String> NO_TERMINAL = new HashSet<>(Arrays.asList(
            "ConnectivityNode",
            "TopologicalNode",
            "Terminal",
            "Substation",
            "Feeder",
            "GeographicalRegion",
            "SubGeographicalRegion"
    ));

    private static final String PARENT_REF_KEY = "Milsoft.parent";
    // Encoded form actually stored in MongoDB (dots → fullwidth period).
    private static final String PARENT_REF_KEY_ENCODED =
            MapKeySanitizer.encode(PARENT_REF_KEY);
    private static final String ROOT_SENTINEL = "ROOT";

    /** Batch size for chunked Mongo reads/writes — same as ImportService default. */
    private static final int BATCH = 500;

    private final CimObjectRecordRepository objectRepo;
    private final ValidationJobRepository    jobRepo;

    public CimTopologySynthesisService(CimObjectRecordRepository objectRepo,
                                        ValidationJobRepository    jobRepo) {
        this.objectRepo = objectRepo;
        this.jobRepo    = jobRepo;
    }

    /**
     * Synthesize CIM topology for one import job.
     *
     * @return {@link SynthesisResult} with counts of objects examined and emitted.
     */
    public SynthesisResult synthesize(String jobId) {
        long t0 = System.currentTimeMillis();
        var job = jobRepo.findByJobId(jobId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Job not found: " + jobId));
        String orgId   = job.getOrgId()          != null ? job.getOrgId()          : "";
        String version = job.getNetworkVersion() != null ? job.getNetworkVersion() : "";
        log.info("Topology synthesis START job={} orgId={} version={}",
                jobId, orgId, version);

        // ── Step 1: clear any previously synthesized objects for this job ────
        // Idempotence: re-running synthesize should not duplicate.
        long deleted = deletePriorSynthesized(jobId);
        log.info("Cleared {} previously-synthesized objects for job={}",
                deleted, jobId);

        // ── Step 2: collect rdfId → cimType + parent for every parsed object ─
        // We need the full topology map up-front to compute ConnectivityNode
        // sharing.  For a 116k-row file this is ~120k rows × ~80 bytes ≈ 10MB
        // in memory — acceptable.  If files grow far beyond this, switch to
        // a two-pass approach: pass 1 builds a parent-child counter, pass 2
        // emits.  Not needed today.
        Map<String, String> rdfIdToType   = new HashMap<>();
        Map<String, String> rdfIdToParent = new HashMap<>();
        // Track which rdfIds appear as someone's parent (i.e. are not pure leaves).
        Set<String> hasChild = new HashSet<>();

        int page = 0;
        long examined = 0;
        while (true) {
            var slice = objectRepo.findByJobId(jobId, PageRequest.of(page, BATCH));
            if (slice.isEmpty()) break;
            for (CimObjectRecord rec : slice.getContent()) {
                if (rec.isSynthesized()) continue;  // belt-and-braces; deleted above
                examined++;
                rdfIdToType.put(rec.getRdfId(), rec.getCimType());
                String parent = parentOf(rec);
                if (parent != null) {
                    rdfIdToParent.put(rec.getRdfId(), parent);
                    hasChild.add(parent);
                }
            }
            if (!slice.hasNext()) break;
            page++;
        }
        log.info("Examined {} parsed objects for job={}", examined, jobId);

        // ── Step 3: assign UUIDs to existing equipment that lacks an mRID ────
        // We update parsed rows in place so cim_objects rows have proper mRIDs
        // (the synthesized Terminals will reference them by UUID).
        long mridsFilled = backfillMrids(jobId);
        log.info("Backfilled mRID on {} parsed objects for job={}",
                mridsFilled, jobId);

        // ── Step 4: walk the parsed objects again and emit synthetic objects ─
        // We process in batches.  For each parsed equipment row, we may emit:
        //   • 1 or 2 Terminal records
        //   • the ConnectivityNode at its parent boundary (if not already emitted)
        //   • a "root" or "leaf" ConnectivityNode where applicable
        // Emitted ConnectivityNode rdfIds are tracked in `emittedCns` so we
        // don't emit duplicates within the same run.
        Set<String> emittedCns = new HashSet<>();
        List<CimObjectRecord> buffer = new ArrayList<>(BATCH);
        long emittedTerminals = 0;
        long emittedCnsCount  = 0;

        page = 0;
        while (true) {
            var slice = objectRepo.findByJobId(jobId, PageRequest.of(page, BATCH));
            if (slice.isEmpty()) break;
            for (CimObjectRecord rec : slice.getContent()) {
                if (rec.isSynthesized()) continue;
                if (NO_TERMINAL.contains(rec.getCimType())) continue;

                String parentRdfId = parentOf(rec);
                int portCount = portCountFor(rec.getCimType());

                // ── Source-side CN (or single CN for one-port devices) ──
                // For one-port devices: the single terminal connects to the
                // ConnectivityNode at the parent boundary (or a root CN if
                // parent==ROOT).
                // For two-port devices: source terminal → CN at parent boundary.
                String sourceCnRdfId = cnRdfIdAt(parentRdfId);
                if (emittedCns.add(sourceCnRdfId)) {
                    buffer.add(buildConnectivityNode(
                            sourceCnRdfId, jobId, orgId, version,
                            rec.getSourceFormat(), rec.getSourceFile()));
                    emittedCnsCount++;
                }

                if (portCount == 1) {
                    // Single terminal, references sourceCn.
                    buffer.add(buildTerminal(
                            "milsoft:term:" + rec.getRdfId(),
                            rec, sourceCnRdfId,
                            "single", jobId, orgId, version));
                    emittedTerminals++;
                } else {
                    // Two terminals: source + load.
                    buffer.add(buildTerminal(
                            "milsoft:term:source:" + rec.getRdfId(),
                            rec, sourceCnRdfId,
                            "source", jobId, orgId, version));
                    emittedTerminals++;

                    // Load-side CN: shared with any children of THIS equipment.
                    // If this equipment has no children, we still create a CN
                    // at its load side — valid CIM, just an empty junction.
                    String loadCnRdfId;
                    if (hasChild.contains(rec.getRdfId())) {
                        loadCnRdfId = cnRdfIdAt(rec.getRdfId());
                    } else {
                        loadCnRdfId = "milsoft:cn:leaf:" + rec.getRdfId();
                    }
                    if (emittedCns.add(loadCnRdfId)) {
                        buffer.add(buildConnectivityNode(
                                loadCnRdfId, jobId, orgId, version,
                                rec.getSourceFormat(), rec.getSourceFile()));
                        emittedCnsCount++;
                    }
                    buffer.add(buildTerminal(
                            "milsoft:term:load:" + rec.getRdfId(),
                            rec, loadCnRdfId,
                            "load", jobId, orgId, version));
                    emittedTerminals++;
                }

                if (buffer.size() >= BATCH) {
                    objectRepo.saveAll(buffer);
                    buffer.clear();
                }
            }
            if (!slice.hasNext()) break;
            page++;
        }
        if (!buffer.isEmpty()) {
            objectRepo.saveAll(buffer);
            buffer.clear();
        }

        long elapsed = System.currentTimeMillis() - t0;
        log.info("Topology synthesis DONE job={} examined={} terminals={} "
                + "connectivityNodes={} mridsFilled={} elapsed={}ms",
                jobId, examined, emittedTerminals, emittedCnsCount,
                mridsFilled, elapsed);

        return new SynthesisResult(jobId, examined, emittedTerminals,
                                    emittedCnsCount, mridsFilled, elapsed);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Parent rdfId of a record, or null if root.  References are stored
     * dot-encoded in MongoDB — we check the encoded key.  Milsoft.parent is
     * single-valued by construction (each row has at most one parent), so we
     * take the first element of the list.
     */
    private String parentOf(CimObjectRecord rec) {
        Map<String, List<String>> refs = rec.getReferences();
        if (refs == null || refs.isEmpty()) return null;
        List<String> parentList = refs.get(PARENT_REF_KEY_ENCODED);
        if (parentList == null) parentList = refs.get(PARENT_REF_KEY);
        if (parentList == null || parentList.isEmpty()) return null;
        String parent = parentList.get(0);
        if (parent == null || parent.isEmpty()) return null;
        if (ROOT_SENTINEL.equalsIgnoreCase(parent)) return null;
        return parent;
    }

    private int portCountFor(String cimType) {
        if (TWO_PORT.contains(cimType)) return 2;
        if (ONE_PORT.contains(cimType)) return 1;
        // Unknown classes: be conservative, give them one terminal so they
        // appear in the topology graph but don't propagate flow through.
        return 1;
    }

    /**
     * ConnectivityNode rdfId at the load side of a parent (= source side
     * of the parent's children).  Shared by all siblings.
     */
    private String cnRdfIdAt(String parentRdfId) {
        if (parentRdfId == null) {
            // Should never happen in a well-formed file (every non-root row
            // has a parent), but defend against malformed data.
            return "milsoft:cn:unknown";
        }
        // Roots — equipment whose parent is "ROOT" — get a CN tagged
        // explicitly so they don't accidentally share with siblings of a
        // non-root with the same id.  Practical case: BU's source terminal.
        if (ROOT_SENTINEL.equalsIgnoreCase(parentRdfId)) {
            return "milsoft:cn:ROOT";
        }
        return "milsoft:cn:" + parentRdfId;
    }

    /**
     * Build a Terminal record referencing the given equipment + ConnectivityNode.
     *
     * @param terminalSeed deterministic seed string for this terminal's UUID
     * @param equipment    the parsed equipment record this terminal belongs to
     * @param cnRdfId      seed string of the ConnectivityNode this terminal connects to
     * @param sequenceTag  "source" / "load" / "single" — stored as Terminal.sequenceNumber hint
     */
    private CimObjectRecord buildTerminal(String terminalSeed,
                                           CimObjectRecord equipment,
                                           String cnRdfId,
                                           String sequenceTag,
                                           String jobId,
                                           String orgId,
                                           String version) {
        String uuid       = uuidFromSeed(terminalSeed);
        String equipUuid  = equipment.getMrid() != null && !equipment.getMrid().isEmpty()
                ? equipment.getMrid()
                : uuidFromSeed("milsoft:equipment:" + equipment.getRdfId());
        String cnUuid     = uuidFromSeed(cnRdfId);

        CimObjectRecord t = new CimObjectRecord();
        t.setJobId(jobId);
        t.setRdfId(terminalSeed);             // human-readable; matches reference target convention
        t.setMrid(uuid);
        t.setCimType("Terminal");
        t.setName(sequenceTag + ":" + equipment.getRdfId());
        t.setCategory("LOGICAL");
        t.setRequired(false);
        t.setSourceFormat(equipment.getSourceFormat());
        t.setSourceFile(equipment.getSourceFile());
        t.setOrgId(orgId);
        t.setVersion(version);
        t.setSynthesized(true);

        Map<String,String> attrs = new LinkedHashMap<>();
        attrs.put(MapKeySanitizer.encode("IdentifiedObject.mRID"), uuid);
        attrs.put(MapKeySanitizer.encode("IdentifiedObject.name"),
                sequenceTag + ":" + equipment.getRdfId());
        attrs.put(MapKeySanitizer.encode("ACDCTerminal.sequenceNumber"),
                "source".equals(sequenceTag) ? "1"
                        : "load".equals(sequenceTag) ? "2" : "1");
        // Phase code inherited from parent equipment, if present.
        String parentPhases = equipment.getAttributes()
                .get(MapKeySanitizer.encode("ACDCTerminal.phases"));
        if (parentPhases != null) {
            attrs.put(MapKeySanitizer.encode("ACDCTerminal.phases"), parentPhases);
        }
        t.setAttributes(attrs);

        Map<String, List<String>> refs = new LinkedHashMap<>();
        // The "target" of these references is the rdfId of the related object.
        // Reference resolution (existing pipeline) will turn these into UUIDs
        // in resolvedRefIds, matching the same convention as Milsoft.parent.
        // Synthesized Terminals have exactly one Equipment and one
        // ConnectivityNode, so the lists are singletons — but the shape must
        // match the multi-valued reference convention.
        refs.put(MapKeySanitizer.encode("Terminal.ConductingEquipment"),
                new ArrayList<>(java.util.Collections.singletonList(equipment.getRdfId())));
        refs.put(MapKeySanitizer.encode("Terminal.ConnectivityNode"),
                new ArrayList<>(java.util.Collections.singletonList(cnRdfId)));
        t.setReferences(refs);

        // Also populate resolvedRefIds directly with UUIDs — we already know
        // them deterministically, so we save the resolver a round trip.
        Map<String, List<String>> resolved = new LinkedHashMap<>();
        resolved.put(MapKeySanitizer.encode("Terminal.ConductingEquipment"),
                new ArrayList<>(java.util.Collections.singletonList(equipUuid)));
        resolved.put(MapKeySanitizer.encode("Terminal.ConnectivityNode"),
                new ArrayList<>(java.util.Collections.singletonList(cnUuid)));
        t.setResolvedRefIds(resolved);

        return t;
    }

    private CimObjectRecord buildConnectivityNode(String cnRdfId,
                                                   String jobId,
                                                   String orgId,
                                                   String version,
                                                   String sourceFormat,
                                                   String sourceFile) {
        String uuid = uuidFromSeed(cnRdfId);
        CimObjectRecord c = new CimObjectRecord();
        c.setJobId(jobId);
        c.setRdfId(cnRdfId);
        c.setMrid(uuid);
        c.setCimType("ConnectivityNode");
        c.setName(cnRdfId);
        c.setCategory("LOGICAL");
        c.setRequired(false);
        c.setSourceFormat(sourceFormat);
        c.setSourceFile(sourceFile);
        c.setOrgId(orgId);
        c.setVersion(version);
        c.setSynthesized(true);

        Map<String,String> attrs = new LinkedHashMap<>();
        attrs.put(MapKeySanitizer.encode("IdentifiedObject.mRID"), uuid);
        attrs.put(MapKeySanitizer.encode("IdentifiedObject.name"), cnRdfId);
        c.setAttributes(attrs);

        return c;
    }

    /**
     * Backfill {@code mRID} on every parsed (non-synthesized) record that
     * lacks one.  Uses {@code "milsoft:equipment:" + rdfId} as the seed so
     * the UUID matches what synthesized Terminals reference.
     *
     * Returns the count of records updated.
     */
    private long backfillMrids(String jobId) {
        long updated = 0;
        List<CimObjectRecord> buffer = new ArrayList<>(BATCH);
        int page = 0;
        while (true) {
            var slice = objectRepo.findByJobId(jobId, PageRequest.of(page, BATCH));
            if (slice.isEmpty()) break;
            for (CimObjectRecord rec : slice.getContent()) {
                if (rec.isSynthesized()) continue;
                String current = rec.getMrid();
                if (current != null && !current.isEmpty() && looksLikeUuid(current)) {
                    continue;   // already has a UUID
                }
                String seed = "milsoft:equipment:" + rec.getRdfId();
                String uuid = uuidFromSeed(seed);
                rec.setMrid(uuid);
                // Also reflect in the attributes map so downstream consumers
                // querying IdentifiedObject.mRID see it.
                Map<String,String> attrs = rec.getAttributes();
                if (attrs == null) attrs = new LinkedHashMap<>();
                attrs.put(MapKeySanitizer.encode("IdentifiedObject.mRID"), uuid);
                rec.setAttributes(attrs);
                buffer.add(rec);
                updated++;
                if (buffer.size() >= BATCH) {
                    objectRepo.saveAll(buffer);
                    buffer.clear();
                }
            }
            if (!slice.hasNext()) break;
            page++;
        }
        if (!buffer.isEmpty()) objectRepo.saveAll(buffer);
        return updated;
    }

    /** True if the string parses as a UUID; cheap check on first char + length. */
    private boolean looksLikeUuid(String s) {
        if (s == null || s.length() != 36) return false;
        try { UUID.fromString(s); return true; } catch (Exception e) { return false; }
    }

    /**
     * Deterministic UUID from a name seed.  Java's
     * {@link UUID#nameUUIDFromBytes} produces a v3 (MD5) name-based UUID;
     * v5 (SHA-1) would be marginally better cryptographically but is not
     * in the standard library and v3 is identical for our purposes
     * (deterministic, same input → same UUID).
     */
    private static String uuidFromSeed(String seed) {
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8))
                .toString();
    }

    /**
     * Delete previously-synthesized rows for this job.  Uses page-based
     * iteration because we don't have a repository method that targets
     * synthesized rows directly; this is fine for batch sizes typical of
     * a single job (tens of thousands at most).
     */
    private long deletePriorSynthesized(String jobId) {
        long deleted = 0;
        int page = 0;
        List<CimObjectRecord> toDelete = new ArrayList<>(BATCH);
        while (true) {
            var slice = objectRepo.findByJobId(jobId, PageRequest.of(page, BATCH));
            if (slice.isEmpty()) break;
            for (CimObjectRecord rec : slice.getContent()) {
                if (rec.isSynthesized()) {
                    toDelete.add(rec);
                    deleted++;
                    if (toDelete.size() >= BATCH) {
                        objectRepo.deleteAll(toDelete);
                        toDelete.clear();
                    }
                }
            }
            if (!slice.hasNext()) break;
            page++;
        }
        if (!toDelete.isEmpty()) objectRepo.deleteAll(toDelete);
        return deleted;
    }

    /** Lightweight result holder for the controller's response body. */
    public static final class SynthesisResult {
        public final String jobId;
        public final long parsedObjectsExamined;
        public final long terminalsEmitted;
        public final long connectivityNodesEmitted;
        public final long mridsBackfilled;
        public final long elapsedMs;
        public SynthesisResult(String jobId, long examined, long terminals,
                                long cns, long mrids, long elapsed) {
            this.jobId                    = jobId;
            this.parsedObjectsExamined    = examined;
            this.terminalsEmitted         = terminals;
            this.connectivityNodesEmitted = cns;
            this.mridsBackfilled          = mrids;
            this.elapsedMs                = elapsed;
        }
    }
}
