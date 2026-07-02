Set-Location "c:\Users\AliCom\PycharmProjects\musiqa bot"
while ($true) {
    $date = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$date] Starting bot..." | Out-File -FilePath "run_bot_stdout.log" -Append
    try {
        & .venv\Scripts\python.exe main.py *>> run_bot_stdout.log
    } catch {
        "[$date] Error: $_" | Out-File -FilePath "run_bot_stdout.log" -Append
    }
    "[$date] Bot exited. Restarting in 5 seconds..." | Out-File -FilePath "run_bot_stdout.log" -Append
    Start-Sleep -Seconds 5
}
