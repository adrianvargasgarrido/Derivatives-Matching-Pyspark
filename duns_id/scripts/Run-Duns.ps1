# =============================================================================
# Run-Duns.ps1  —  Quick launcher
# =============================================================================
# Usage:
#   .\Run-Duns.ps1          →  Preview DEV  (stats + optional download)
#   .\Run-Duns.ps1 test     →  Preview TEST (stats + optional download)
#   .\Run-Duns.ps1 export   →  Export all   (DEV, no preview)
#   .\Run-Duns.ps1 export test  →  Export all (TEST, no preview)
# =============================================================================

param(
    [string]$Action      = "preview",   # preview | export
    [string]$Environment = "dev"        # dev | test
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Env       = $Environment.ToUpper()

switch ($Action.ToLower()) {

    "preview" {
        & "$ScriptDir\Preview-DunsCollections.ps1" -Environment $Env
    }

    "export" {
        & "$ScriptDir\Export-DunsCollections.ps1" -Environment $Env
    }

    default {
        Write-Host "Usage: .\Run-Duns.ps1 [preview|export] [dev|test]" -ForegroundColor Yellow
        Write-Host "  .\Run-Duns.ps1                  -> preview DEV"
        Write-Host "  .\Run-Duns.ps1 test             -> preview TEST"
        Write-Host "  .\Run-Duns.ps1 export           -> export DEV"
        Write-Host "  .\Run-Duns.ps1 export test      -> export TEST"
    }
}
