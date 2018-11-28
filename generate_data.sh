#!/bin/sh
mkdir druid_data
if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"; pwd)"
fi
pwd
echo $BENCHMARK_HOME

MAIN_CLASS=cn.edu.ruc.utils.GenerateData

CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi
exec "$JAVA"   -cp "$CLASSPATH" "$MAIN_CLASS" "7"

