@echo off
REM AIVectorMemory Desktop App Installer (Windows)

echo === AIVectorMemory Installer ===
echo.

set "DEST=%USERPROFILE%\.aivectormemory"
set "SCRIPT_DIR=%~dp0"

REM Create destination directory
if not exist "%DEST%" mkdir "%DEST%"

REM Copy sqlite-vec extension
if exist "%SCRIPT_DIR%vec0.dll" (
    copy /Y "%SCRIPT_DIR%vec0.dll" "%DEST%\vec0.dll" >nul
    echo Installed vec0.dll -^> %DEST%\vec0.dll
) else (
    echo Warning: vec0.dll not found in package, skipping
)

REM Copy executable
if exist "%SCRIPT_DIR%AIVectorMemory.exe" (
    set "INSTALL_DIR=%LOCALAPPDATA%\AIVectorMemory"
    if not exist "!INSTALL_DIR!" mkdir "!INSTALL_DIR!"
    copy /Y "%SCRIPT_DIR%AIVectorMemory.exe" "%LOCALAPPDATA%\AIVectorMemory\AIVectorMemory.exe" >nul
    echo Installed AIVectorMemory.exe -^> %LOCALAPPDATA%\AIVectorMemory\
)

echo.
echo Installation complete!
pause
