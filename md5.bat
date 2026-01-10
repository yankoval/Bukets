@echo off
:: Проверяем количество переданных параметров
if "%~1"=="" (
    echo Параметры не найдены. 
	echo Usage: md5.bat file-name
	) else (
	powershell -Command "$md5=(Get-FileHash %1 -Algorithm MD5).Hash.ToLower(); [convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($md5))" 
	)