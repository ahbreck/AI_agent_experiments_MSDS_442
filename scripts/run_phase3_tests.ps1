param(
    [string]$EnvName = "msds_442",
    [string[]]$Tests = @(
        "Project_Phase_3.tests.test_membership_fraud_story2",
        "Project_Phase_3.tests.test_orchestrator_membership_fraud_routing",
        "Project_Phase_3.tests.test_business_marketing_story3"
    )
)

$ErrorActionPreference = "Stop"

function Invoke-Phase3Python {
    param(
        [string]$Env,
        [string[]]$PyArgs
    )

    if ($env:CONDA_DEFAULT_ENV -eq $Env) {
        & python @PyArgs
        return $LASTEXITCODE
    }

    $conda = Get-Command conda -ErrorAction SilentlyContinue
    if ($conda) {
        & conda run -n $Env python @PyArgs
        return $LASTEXITCODE
    }

    $candidates = @(
        (Join-Path $env:USERPROFILE "anaconda3\envs\$Env\python.exe"),
        (Join-Path $env:USERPROFILE "miniconda3\envs\$Env\python.exe"),
        (Join-Path $env:LOCALAPPDATA "anaconda3\envs\$Env\python.exe"),
        (Join-Path $env:LOCALAPPDATA "miniconda3\envs\$Env\python.exe")
    )
    $pyExe = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($pyExe) {
        & $pyExe @PyArgs
        return $LASTEXITCODE
    }

    throw "Could not find conda env '$Env'. Activate it first or install it from environment.yml."
}

$argsList = @("-m", "unittest") + $Tests
$exitCode = Invoke-Phase3Python -Env $EnvName -PyArgs $argsList
exit $exitCode
