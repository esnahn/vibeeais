# .py 노트북을 ipynb로 변환 → 실행 → HTML 출력 → ipynb 삭제
param([string]$PyFile)

if (-not $PyFile) {
    Write-Host "Usage: .\scripts\run_notebook.ps1 notebooks\<name>.py" -ForegroundColor Yellow
    exit 0
}

$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = 1

# Resolve paths
$pyPath   = Resolve-Path $PyFile
$baseName = [System.IO.Path]::GetFileNameWithoutExtension($pyPath)
$dir      = Split-Path $pyPath
$ipynb    = Join-Path $dir "$baseName.ipynb"
$htmlName = "${baseName}.html"

# 1. Remove stale ipynb if exists
Remove-Item $ipynb -ErrorAction SilentlyContinue

# 2. Convert .py -> .ipynb
Write-Host "[1/3] jupytext --to ipynb $pyPath" -ForegroundColor Cyan
& .\.venv\Scripts\jupytext.exe --to ipynb $pyPath
if ($LASTEXITCODE -ne 0) { throw "jupytext failed" }

# 3. Execute and export to HTML
Write-Host "[2/3] nbconvert --execute $ipynb -> $htmlName" -ForegroundColor Cyan
& .\.venv\Scripts\jupyter-nbconvert.exe --to html --execute $ipynb --output $htmlName
if ($LASTEXITCODE -ne 0) { throw "nbconvert failed" }

# 4. Cleanup: remove generated ipynb
Write-Host "[3/3] Removing $ipynb" -ForegroundColor Cyan
Remove-Item $ipynb -ErrorAction SilentlyContinue

Write-Host "Done: $htmlName" -ForegroundColor Green
