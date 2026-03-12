# =============================================================================
# DunsConfig.ps1
# -----------------------------------------------------------------------------
# Shared configuration for all DUNS ID scripts.
# Sourced (dot-sourced) by Preview-DunsCollections.ps1 and
# Export-DunsCollections.ps1 so there is a SINGLE place to maintain:
#   - The list of collections
#   - MongoDB connection details per environment
#   - Tool paths and credentials
# =============================================================================

# =============================================================================
# 1. COLLECTION LIST
#    Add / remove collection names here. Both Preview and Export scripts
#    iterate over this list.
# =============================================================================
$DunsCollections = @(
    "BARC_CLNT_OWNRSHP_HRCHY_duns"
    # "BARC_ENTITY_MASTER"
    # "BARC_RELATIONSHIP_DATA"
    # "BARC_ANOTHER_COLLECTION"
    # Add more collections here as needed
)

# =============================================================================
# 2. ENVIRONMENT CONFIG
#    Maps DEV / TEST to their respective MongoDB URIs and database names.
#    NOTE: The URI for mongosh uses a DIFFERENT host than mongoexport.
#          Update both if the hosts change.
# =============================================================================
$DunsEnvConfig = @{
    DEV  = @{
        # mongosh connects to this host
        MongoshUri   = "mongodb://duwdsr002197096.intranet.barcapint.com:27120/?replicaSet=repl&readPreference=nearest&tls=true"
        # mongoexport connects to this host
        ExportUri    = "mongodb://duwdsr002197094.intranet.barcapint.com:27120/?replicaSet=repl&readPreference=nearest&tls=true"
        Database     = "BARC_DNB_ODS_DEV"
    }
    TEST = @{
        MongoshUri   = "mongodb://duwdsr002197096.intranet.barcapint.com:27120/?replicaSet=repl&readPreference=nearest&tls=true"
        ExportUri    = "mongodb://duwdsr002197094.intranet.barcapint.com:27120/?replicaSet=repl&readPreference=nearest&tls=true"
        Database     = "BARC_DNB_ODS_TEST"
    }
}

# =============================================================================
# 3. TOOL PATHS
# =============================================================================
$DunsMongoshPath     = "C:\APPS\MongoDB\7.0.16\mongosh-2.2.9-win32-x64\bin\mongosh.exe"
$DunsMongoExportPath = "C:\APPS\MongoDB\7.0.16\mongodb-database-tools-windows-x86_64-100.10.0\bin\mongoexport.exe"
$DunsSslCAFile       = "C:\Users\x01594717\Downloads\ca_all.pem"

# =============================================================================
# 4. CREDENTIALS
# =============================================================================
$DunsUsername = "x01594717"
