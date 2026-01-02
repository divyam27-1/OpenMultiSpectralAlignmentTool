@echo off
set VERSION=1.1.1
set DIST_NAME=OMSPEC_v%VERSION%
set DIST_DIR=dist\%DIST_NAME%

echo [1/4] Cleaning old distribution...
if exist %DIST_DIR% rd /s /q %DIST_DIR%
timeout /t 2 /nobreak > nul

echo [2/4] Building Release Binary...
dub build -b release --compiler=dmd

echo [3/4] Creating Folder Structure...
mkdir %DIST_DIR%
mkdir %DIST_DIR%\engine
mkdir %DIST_DIR%\bin
mkdir %DIST_DIR%\python_3_11_14

echo [4/4] Copying Runtime Dependencies...
:: Copy the executable and user configuration
copy bin\omspec.exe %DIST_DIR%\bin
copy omspec.cfg %DIST_DIR%\bin

:: Copy Python scripts (the Engine)
robocopy engine %DIST_DIR%\engine /E /R:0 /W:0 /NFL /NDL /NJH /NJS /NP
echo "do not edit the engine files or python runtime files. if you edit them you are intentionally hampering the stability and complete functionality of the omspec application" > %DIST_DIR%\engine\DO_NOT_TOUCH.txt
attrib +R %DIST_DIR%\engine\*.py /S

:: Copy the embedded Python interpreter (assuming it's in your project root)
:: We use /E /I to copy directories and subdirectories
robocopy python_3_11_14 %DIST_DIR%\python_3_11_14 /E /R:0 /W:0 /COPY:DAT /DCOPY:DAT /NFL /NDL /NJH /NJS /NP

:: Copy the License
copy LICENSE.txt %DIST_DIR%

:: Copy the README
copy README.md %DIST_DIR%

echo ---------------------------------------------------
echo Build Complete: %DIST_DIR%
echo You can now ZIP this folder for distribution.
echo ---------------------------------------------------
pause