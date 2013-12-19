SETLOCAL

SET BaseDir=%~dp0
SET FrameworkVersion=v4.0.30319
SET FrameworkDir=%SystemRoot%\Microsoft.NET\Framework
SET LibsDir=%BaseDir%Libs\

SET Flavor=Release
IF [%1] NEQ [] (SET Flavor=%~1)

RMDIR /S /Q "%LibsDir%NativeBinaries"
DEL "%LibsDir%LibGit2Sharp.*"
DEL "%LibsDir%CopyNativeDependencies.targets"
DEL "%LibsDir%Voron.*"

PUSHD "%BaseDir%libgit2sharp"
"%FrameworkDir%\%FrameworkVersion%\msbuild.exe" "CI-build.msbuild" /target:Build /property:Configuration=%Flavor%

XCOPY ".\LibGit2Sharp\bin\%Flavor%\*.*" "%LibsDir%" /S /Y
XCOPY ".\LibGit2Sharp\CopyNativeDependencies.targets" "%LibsDir%" /S /Y

POPD

PUSHD "%BaseDir%raven.voron"
"%FrameworkDir%\%FrameworkVersion%\msbuild.exe" "Voron\Voron.csproj" /property:Configuration=%Flavor%

XCOPY ".\Voron\bin\%Flavor%\*.*" "%LibsDir%" /S /Y

POPD

ENDLOCAL

EXIT /B %ERRORLEVEL%