
SETLOCAL ENABLEDELAYEDEXPANSION

PUSHD %~dp0..
IF NOT DEFINED RUCTS_HOME SET RUCTS_HOME=%CD%
POPD
"%CD%"
SET CLASSPATH=
SET RUCTS_CLASS="cn.edu.ruc.biz.Core"
SET STARUP=
IF "%JAVA_HOME%." == "." GOTO noJavaHome
IF NOT EXIST "%JAVA_HOME%\bin\java.exe" GOTO noJavaHome
GOTO okJava
:noJavaHome

ECHO The JAVA_HOME environment variable is not defined correctly.
GOTO exit
:okJava

IF NOT "import" == "%1" GOTO noimport
SET STARUP="load.online"
GOTO gotCommand
:noimport
IF NOT "perform" == "%1" GOTO noShell
SET STARUP="perform"
GOTO gotCommand
:noShell
ECHO [ERROR] Found unknown command '%1'
ECHO [ERROR] Expected one of 'import', 'perform'. Exiting.
GOTO exit
:gotCommand
FOR /F "delims=" %%G in (
  'FINDSTR /B "%2:" %RUCTS_HOME%\build\bindings.properties'
) DO SET "BINDING_LINE=%%G"
IF NOT "%BINDING_LINE%." == "." GOTO gotBindingLine
ECHO [ERROR] The specified binding '%2' was not found.  Exiting.
GOTO exit

:gotBindingLine

@REM Pull out binding name and class
FOR /F "tokens=1-2 delims=:" %%G IN ("%BINDING_LINE%") DO (
  SET BINDING_NAME=%%G
  SET BINDING_CLASS=%%H
)
FOR /F "tokens=1 delims=-" %%G IN ("%BINDING_NAME%") DO (
  SET BINDING_DIR=%%G
)

@REM Database libraries
FOR %%F IN (%RUCTS_HOME%\bm-%BINDING_DIR%\target\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

@REM Database dependency libraries
FOR %%F IN (%RUCTS_HOME%\bm-%BINDING_DIR%\target\lib\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

FOR /F "tokens=2*" %%G IN ("%*") DO (
  SET YCSB_ARGS=%%H
)

@ECHO ON
"%JAVA_HOME%\bin\java.exe" %JAVA_OPTS% -classpath "%CLASSPATH%" %RUCTS_CLASS% %RUCTS_COMMAND% -db %BINDING_CLASS% -starup %STARUP% %RUCTS_ARGS%
@ECHO OFF

GOTO end

:exit
EXIT /B 1;

:end