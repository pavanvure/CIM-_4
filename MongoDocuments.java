package com.cim.model.mongo;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.*;

// ═══════════════════════════════════════════════════════════════════
// ValidationJob — one per import operation
// ═══════════════════════════════════════════════════════════════════
@Document(collection = "validation_jobs")
class ValidationJob {
    @Id private String id;
    @Indexed(unique = true) private String jobId;
    private String fileName;
    private String fileFormat;
    private String fileHash;
    private String status;          // PENDING, RUNNING, RESOLVED, FAILED
    private String networkVersion;
    private String orgId;
    private String submittedBy;
    private long   totalObjects;
    private long   physicalObjects;
    private long   logicalObjects;
    private long   pathIssueCount;
    private long   processingMs;
    private StageTimings stageTimings;
    private List<String> enabledNamespaces = new ArrayList<>();
    private String errorMessage;
    private Instant submittedAt;
    private Instant completedAt;

    public ValidationJob() {}
    public ValidationJob(String jobId, String fileName, String format,
                          String submittedBy, String networkVersion, String orgId) {
        this.jobId          = jobId;
        this.fileName       = fileName;
        this.fileFormat     = format;
        this.submittedBy    = submittedBy;
        this.networkVersion = networkVersion;
        this.orgId          = orgId;
        this.status         = "PENDING";
        this.submittedAt    = Instant.now();
        this.stageTimings   = new StageTimings();
    }

    public String       getId()                          { return id; }
    public String       getJobId()                       { return jobId; }
    public void         setJobId(String v)               { this.jobId = v; }
    public String       getFileName()                    { return fileName; }
    public void         setFileName(String v)            { this.fileName = v; }
    public String       getFileFormat()                  { return fileFormat; }
    public void         setFileFormat(String v)          { this.fileFormat = v; }
    public String       getFileHash()                    { return fileHash; }
    public void         setFileHash(String v)            { this.fileHash = v; }
    public String       getStatus()                      { return status; }
    public void         setStatus(String v)              { this.status = v; }
    public String       getNetworkVersion()              { return networkVersion; }
    public void         setNetworkVersion(String v)      { this.networkVersion = v; }
    public String       getOrgId()                       { return orgId; }
    public void         setOrgId(String v)               { this.orgId = v; }
    public String       getSubmittedBy()                 { return submittedBy; }
    public long         getTotalObjects()                { return totalObjects; }
    public void         setTotalObjects(long v)          { this.totalObjects = v; }
    public long         getPhysicalObjects()             { return physicalObjects; }
    public void         setPhysicalObjects(long v)       { this.physicalObjects = v; }
    public long         getLogicalObjects()              { return logicalObjects; }
    public void         setLogicalObjects(long v)        { this.logicalObjects = v; }
    public long         getPathIssueCount()              { return pathIssueCount; }
    public void         setPathIssueCount(long v)        { this.pathIssueCount = v; }
    public long         getProcessingMs()                { return processingMs; }
    public void         setProcessingMs(long v)          { this.processingMs = v; }
    public StageTimings getStageTimings()                { return stageTimings; }
    public void         setStageTimings(StageTimings v)  { this.stageTimings = v; }
    public List<String> getEnabledNamespaces()           { return enabledNamespaces; }
    public void         setEnabledNamespaces(List<String> v) { this.enabledNamespaces = v; }
    public String       getErrorMessage()                { return errorMessage; }
    public void         setErrorMessage(String v)        { this.errorMessage = v; }
    public Instant      getSubmittedAt()                 { return submittedAt; }
    public void         setSubmittedAt(Instant v)        { this.submittedAt = v; }
    public Instant      getCompletedAt()                 { return completedAt; }
    public void         setCompletedAt(Instant v)        { this.completedAt = v; }
}

// ═══════════════════════════════════════════════════════════════════
// CimObjectRecord — one per parsed CIM object
// ═══════════════════════════════════════════════════════════════════
@Document(collection = "cim_objects")
@CompoundIndexes({
    @CompoundIndex(name = "idx_jobid",          def = "{'jobId':1}"),
    @CompoundIndex(name = "idx_jobid_rdfid",    def = "{'jobId':1,'rdfId':1}"),
    @CompoundIndex(name = "idx_jobid_cimtype",  def = "{'jobId':1,'cimType':1}"),
    @CompoundIndex(name = "idx_jobid_category", def = "{'jobId':1,'category':1}"),
    @CompoundIndex(name = "idx_jobid_required", def = "{'jobId':1,'isRequired':1}"),
    @CompoundIndex(name = "idx_jobid_orgid",    def = "{'jobId':1,'orgId':1}"),
})
class CimObjectRecord {
    @Id private String id;
    private String jobId;
    private String rdfId;
    private String mrid;
    private String cimType;
    private String name;
    private String category;        // PHYSICAL or LOGICAL
    private boolean isRequired;
    private String sourceFormat;
    private String sourceFile;
    private String orgId;
    private String version;
    private Map<String, String> attributes      = new LinkedHashMap<>();
    private Map<String, String> references      = new LinkedHashMap<>();
    private Map<String, String> resolvedRefIds  = new LinkedHashMap<>();
    private List<Map<String, String>> pathIssues = new ArrayList<>();
    private Instant createdAt;

    public CimObjectRecord() { this.createdAt = Instant.now(); }

    public String  getId()                                { return id; }
    public String  getJobId()                             { return jobId; }
    public void    setJobId(String v)                     { this.jobId = v; }
    public String  getRdfId()                             { return rdfId; }
    public void    setRdfId(String v)                     { this.rdfId = v; }
    public String  getMrid()                              { return mrid; }
    public void    setMrid(String v)                      { this.mrid = v; }
    public String  getCimType()                           { return cimType; }
    public void    setCimType(String v)                   { this.cimType = v; }
    public String  getName()                              { return name; }
    public void    setName(String v)                      { this.name = v; }
    public String  getCategory()                          { return category; }
    public void    setCategory(String v)                  { this.category = v; }
    public boolean isRequired()                           { return isRequired; }
    public void    setRequired(boolean v)                 { this.isRequired = v; }
    public String  getSourceFormat()                      { return sourceFormat; }
    public void    setSourceFormat(String v)              { this.sourceFormat = v; }
    public String  getSourceFile()                        { return sourceFile; }
    public void    setSourceFile(String v)                { this.sourceFile = v; }
    public String  getOrgId()                             { return orgId; }
    public void    setOrgId(String v)                     { this.orgId = v; }
    public String  getVersion()                           { return version; }
    public void    setVersion(String v)                   { this.version = v; }
    public Map<String,String> getAttributes()             { return attributes; }
    public void    setAttributes(Map<String,String> v)    { this.attributes = v != null ? v : new LinkedHashMap<>(); }
    public Map<String,String> getReferences()             { return references; }
    public void    setReferences(Map<String,String> v)    { this.references = v != null ? v : new LinkedHashMap<>(); }
    public Map<String,String> getResolvedRefIds()         { return resolvedRefIds; }
    public void    setResolvedRefIds(Map<String,String> v){ this.resolvedRefIds = v != null ? v : new LinkedHashMap<>(); }
    public List<Map<String,String>> getPathIssues()       { return pathIssues; }
    public void    setPathIssues(List<Map<String,String>> v){ this.pathIssues = v != null ? v : new ArrayList<>(); }
    public Instant getCreatedAt()                         { return createdAt; }
}

// ═══════════════════════════════════════════════════════════════════
// CimClassDefinition — loaded from OWL files
// ═══════════════════════════════════════════════════════════════════
@Document(collection = "cim_standard_owl")
class CimClassDefinition {
    @Id private String id;
    @Indexed(unique = true) private String className;
    private String   parentClass;
    private List<String> ancestors      = new ArrayList<>();
    private List<String> ownAttributes  = new ArrayList<>();
    private String   category;          // PHYSICAL or LOGICAL
    private boolean  required;
    private String   stereotype;
    private String   packageName;
    private String   description;
    private String   owlVersion;

    public CimClassDefinition() {}
    public CimClassDefinition(String className) { this.className = className; }

    public String       getId()                            { return id; }
    public String       getClassName()                     { return className; }
    public void         setClassName(String v)             { this.className = v; }
    public String       getParentClass()                   { return parentClass; }
    public void         setParentClass(String v)           { this.parentClass = v; }
    public List<String> getAncestors()                     { return ancestors; }
    public void         setAncestors(List<String> v)       { this.ancestors = v != null ? v : new ArrayList<>(); }
    public List<String> getOwnAttributes()                 { return ownAttributes; }
    public void         setOwnAttributes(List<String> v)   { this.ownAttributes = v != null ? v : new ArrayList<>(); }
    public String       getCategory()                      { return category; }
    public void         setCategory(String v)              { this.category = v; }
    public boolean      isRequired()                       { return required; }
    public void         setRequired(boolean v)             { this.required = v; }
    public String       getStereotype()                    { return stereotype; }
    public void         setStereotype(String v)            { this.stereotype = v; }
    public String       getPackageName()                   { return packageName; }
    public void         setPackageName(String v)           { this.packageName = v; }
    public String       getDescription()                   { return description; }
    public void         setDescription(String v)           { this.description = v; }
    public String       getOwlVersion()                    { return owlVersion; }
    public void         setOwlVersion(String v)            { this.owlVersion = v; }
}

// ═══════════════════════════════════════════════════════════════════
// NamespaceConfig — caiso, spc, cim namespace settings
// ═══════════════════════════════════════════════════════════════════
@Document(collection = "namespace_configs")
@CompoundIndex(name="ns_org_prefix", def="{'orgId':1,'prefix':1}", unique=true)
class NamespaceConfig {
    @Id private String id;

    /**
     * ENHANCEMENT 1: Organisation-specific namespace config.
     * orgId=null or blank means GLOBAL — applies to all organisations.
     * orgId="CAISO" means only applies when importing CAISO files.
     *
     * Priority during import:
     *   1. Org-specific config (orgId matches import orgId)
     *   2. Global config (orgId is null/blank)
     *   3. Default: disabled (attribute not stored)
     */
    private String  orgId;
    private String  prefix;
    private String  namespaceUri;
    private boolean enabled;
    private boolean validatePath;
    /**
     * REQ 3: Is this a standard CIM namespace (true) or a custom/vendor namespace (false)?
     * true  = IEC CIM standard (cim:, cims:, etc.) — always included in export
     * false = Custom/vendor namespace (caiso:, spc:, ercot:) — exported only
     *         when Neo4jExportConfig.includeNonCimNamespaces = true
     *
     * Default: true (safe — existing namespaces treated as CIM standard)
     * Set to false for: caiso, spc, ercot, vendor-specific prefixes
     */
    private boolean cimStandard = true;
    private String  description;
    private String  createdBy;
    private Instant createdAt;
    private Instant updatedAt;

    public NamespaceConfig() {}
    public NamespaceConfig(String prefix, String uri,
                            boolean enabled, boolean validatePath, String desc) {
        this.prefix       = prefix;
        this.namespaceUri = uri;
        this.enabled      = enabled;
        this.validatePath = validatePath;
        this.cimStandard  = true; // default: treat as CIM standard
        this.description  = desc;
        this.createdAt    = Instant.now();
        this.updatedAt    = Instant.now();
    }

    public String  getId()                        { return id; }
    public String  getOrgId()                     { return orgId; }
    public void    setOrgId(String v)             { this.orgId = v; }
    public String  getPrefix()                    { return prefix; }
    public void    setPrefix(String v)            { this.prefix = v; }
    public String  getNamespaceUri()              { return namespaceUri; }
    public void    setNamespaceUri(String v)      { this.namespaceUri = v; }
    public boolean isEnabled()                    { return enabled; }
    public void    setEnabled(boolean v)          { this.enabled = v; }
    public boolean isValidatePath()               { return validatePath; }
    public void    setValidatePath(boolean v)     { this.validatePath = v; }
    public boolean isCimStandard()                { return cimStandard; }
    public void    setCimStandard(boolean v)      { this.cimStandard = v; }
    public String  getDescription()               { return description; }
    public void    setDescription(String v)       { this.description = v; }
    public String  getCreatedBy()                 { return createdBy; }
    public void    setCreatedBy(String v)         { this.createdBy = v; }
    public Instant getCreatedAt()                 { return createdAt; }
    public void    setCreatedAt(Instant v)        { this.createdAt = v; }
    public Instant getUpdatedAt()                 { return updatedAt; }
    public void    setUpdatedAt(Instant v)        { this.updatedAt = v; }
}

// ═══════════════════════════════════════════════════════════════════
// Neo4jExportJob — tracks Neo4j export progress
// ═══════════════════════════════════════════════════════════════════
@Document(collection = "neo4j_exports")
class Neo4jExportJob {
    @Id private String id;
    @Indexed(unique = true) private String exportId;
    private String jobId;
    private String networkVersion;
    private String orgId;
    private String exportMode;
    private String status;           // PENDING, RUNNING, DONE, FAILED
    private long   nodesExported;
    private long   relsExported;
    private long   processingMs;
    private String errorMessage;
    private Instant startedAt;
    private Instant completedAt;

    public Neo4jExportJob() {}
    public Neo4jExportJob(String exportId, String jobId,
                           String networkVersion, String orgId, String exportMode) {
        this.exportId       = exportId;
        this.jobId          = jobId;
        this.networkVersion = networkVersion;
        this.orgId          = orgId;
        this.exportMode     = exportMode;
        this.status         = "PENDING";
        this.startedAt      = Instant.now();
    }

    public String  getId()                       { return id; }
    public String  getExportId()                 { return exportId; }
    public String  getJobId()                    { return jobId; }
    public String  getNetworkVersion()           { return networkVersion; }
    public void    setNetworkVersion(String v)   { this.networkVersion = v; }
    public String  getOrgId()                    { return orgId; }
    public void    setOrgId(String v)            { this.orgId = v; }
    public String  getExportMode()               { return exportMode; }
    public String  getStatus()                   { return status; }
    public void    setStatus(String v)           { this.status = v; }
    public long    getNodesExported()            { return nodesExported; }
    public void    setNodesExported(long v)      { this.nodesExported = v; }
    public long    getRelsExported()             { return relsExported; }
    public void    setRelsExported(long v)       { this.relsExported = v; }
    public long    getProcessingMs()             { return processingMs; }
    public void    setProcessingMs(long v)       { this.processingMs = v; }
    public String  getErrorMessage()             { return errorMessage; }
    public void    setErrorMessage(String v)     { this.errorMessage = v; }
    public Instant getStartedAt()               { return startedAt; }
    public Instant getCompletedAt()             { return completedAt; }
    public void    setCompletedAt(Instant v)    { this.completedAt = v; }
}

// ═══════════════════════════════════════════════════════════════════
// Neo4jExportConfig — stored per-variant CIM type configs
// ═══════════════════════════════════════════════════════════════════
@Document(collection = "neo4j_export_configs")
class Neo4jExportConfig {
    @Id private String id;
    private String  configName;
    private String  description;
    private String  orgId;
    private String  exportMode;   // kept for backward compat
    private boolean clearFirst;
    private boolean autoExport;
    private boolean active;
    private String  createdBy;
    private Instant createdAt;
    private Instant updatedAt;

    // REQ #3: three separate CIM type lists, one per version variant
    private List<String> fullCimTypes      = new ArrayList<>();  // → {version}-0
    private List<String> sanitisedCimTypes = new ArrayList<>();  // → {version}-1
    private List<String> rationalisedCimTypes = new ArrayList<>();  // → {version}-2

    /** Legacy single list kept for backward compat */
    @Deprecated private List<String> cimTypes = new ArrayList<>();

    /** REQ 6: CIM types for CUSTOM variant — stored as customNeoVersion in Neo4j */
    private List<String> customCimTypes      = new ArrayList<>();
    /** REQ 6: User-provided Neo4j version string for CUSTOM variant */
    private String customNeoVersion;

    /**
     * REQ 3: Include non-CIM namespace attributes in exported Neo4j nodes.
     * Applies to ALL export modes (FULL, SANITISED, RATIONALISED, CUSTOM, TOPOLOGY).
     *
     * true  = export attributes from ALL enabled namespaces
     *         (CIM standard + vendor: caiso:, spc:, ercot: etc.)
     * false = export attributes from CIM standard namespaces only
     *         (namespaces where NamespaceConfig.cimStandard = true)
     *
     * Default: false — standard behaviour is CIM-only attributes in graph
     * Set true when you want vendor-specific attributes visible in Neo4j queries
     */
    private boolean includeNonCimNamespaces = false;

    /**
     * When true, the import pipeline auto-generates a GeoJSON FeatureCollection
     * file at the end of validation (regardless of import format).  When false
     * (default), GeoJSON generation only fires automatically for Milsoft
     * imports — for which it's deemed always-useful — and otherwise only on
     * demand via {@code POST /api/jobs/{jobId}/generate-geojson}.
     */
    private boolean generateGeoJson = false;

    public Neo4jExportConfig() {}

    public String       getId()                              { return id; }
    public String       getConfigName()                      { return configName; }
    public void         setConfigName(String v)              { this.configName = v; }
    public String       getDescription()                     { return description; }
    public void         setDescription(String v)             { this.description = v; }
    public String       getOrgId()                           { return orgId; }
    public void         setOrgId(String v)                   { this.orgId = v; }
    public String       getExportMode()                      { return exportMode; }
    public void         setExportMode(String v)              { this.exportMode = v; }
    public boolean      isClearFirst()                       { return clearFirst; }
    public void         setClearFirst(boolean v)             { this.clearFirst = v; }
    public boolean      isAutoExport()                       { return autoExport; }
    public void         setAutoExport(boolean v)             { this.autoExport = v; }
    public boolean      isActive()                           { return active; }
    public void         setActive(boolean v)                 { this.active = v; }
    public String       getCreatedBy()                       { return createdBy; }
    public void         setCreatedBy(String v)               { this.createdBy = v; }
    public Instant      getCreatedAt()                       { return createdAt; }
    public void         setCreatedAt(Instant v)              { this.createdAt = v; }
    public Instant      getUpdatedAt()                       { return updatedAt; }
    public void         setUpdatedAt(Instant v)              { this.updatedAt = v; }
    public List<String> getFullCimTypes()                    { return fullCimTypes; }
    public void         setFullCimTypes(List<String> v)      { this.fullCimTypes = v != null ? v : new ArrayList<>(); }
    public List<String> getSanitisedCimTypes()               { return sanitisedCimTypes; }
    public void         setSanitisedCimTypes(List<String> v) { this.sanitisedCimTypes = v != null ? v : new ArrayList<>(); }
    public List<String> getRationalisedCimTypes()               { return rationalisedCimTypes; }
    public void         setRationalisedCimTypes(List<String> v) { this.rationalisedCimTypes = v != null ? v : new ArrayList<>(); }
    public List<String> getCimTypes()                            { return cimTypes; }
    public void         setCimTypes(List<String> v)              { this.cimTypes = v != null ? v : new ArrayList<>(); }
    public boolean      isIncludeNonCimNamespaces()              { return includeNonCimNamespaces; }
    public void         setIncludeNonCimNamespaces(boolean v)    { this.includeNonCimNamespaces = v; }
    public boolean      isGenerateGeoJson()                      { return generateGeoJson; }
    public void         setGenerateGeoJson(boolean v)            { this.generateGeoJson = v; }
    public List<String> getCustomCimTypes()                      { return customCimTypes; }
    public void         setCustomCimTypes(List<String> v)        { this.customCimTypes = v != null ? v : new ArrayList<>(); }
    public String       getCustomNeoVersion()                    { return customNeoVersion; }
    public void         setCustomNeoVersion(String v)            { this.customNeoVersion = v; }
}
