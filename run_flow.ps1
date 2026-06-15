$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$dir\.venv\Scripts\python.exe" "$dir\flow\ingestion_flow.py"
