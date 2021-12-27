schtasks /create /tn LONGTERM_PREDICT /tr "\"C:\workpy\APS_SYSTEM\bat_file\new_pred_monday.bat"" /sc weekly /d MON /st 06:00

schtasks /create /sc DAILY /tn daily_morning /tr "\"C:\workpy\APS_SYSTEM\bat_file\morning.bat"" /st 08:00

schtasks /create /sc DAILY /tn daily_evening /tr "\"C:\workpy\APS_SYSTEM\bat_file\afternoon.bat"" /st 23:04


pause