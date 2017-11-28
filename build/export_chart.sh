#!/bin/bash
# Attempt to find the available JAVA, if JAVA_HOME not set
if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=$(which java 2>/dev/null)
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_HOME=$(dirname "$(dirname "$JAVA_PATH" 2>/dev/null)")
  fi
fi

# If JAVA_HOME still not set, error
if [ -z "$JAVA_HOME" ]; then
  echo "[ERROR] Java executable not found. Exiting."
  exit 1;
fi

SCRIPT_DIR=$(dirname "$0" 2>/dev/null)
# Only set RUCTS_HOME if not already set
[ -z "$RUCTS_HOME" ] && RUCTS_HOME=$(cd "$SCRIPT_DIR/.." || exit; pwd)
RUCTS_CLASS='cn.edu.ruc.biz.chart.ChartBizUtil'
# Check if source checkout, or release distribution
DISTRIBUTION=true
if [ -r "$RUCTS_HOME/pom.xml" ]; then
  DISTRIBUTION=false;
fi
BINDING_DIR='bm-core'
# Build classpath
#   The "if" check after the "for" is because glob may just return the pattern
#   when no files are found.  The "if" makes sure the file is really there.
if $DISTRIBUTION; then
  # Database libraries
  for f in "$RUCTS_HOME"/"$BINDING_DIR"/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done
   # Source checkout
else
  # Check for some basic libraries to see if the source has been built.
  for f in "$RUCTS_HOME"/"$BINDING_DIR"/target/lib/*.jar ; do

    # Call mvn to build source checkout.
    if [ ! -e "$f" ] ; then
      MVN_PROJECT="$BINDING_DIR"
	  echo $MVN_PROJECT
	  echo "==================="
      echo "[WARN] ts-benchmark libraries not found.  Attempting to build..."
	  cd ..
      mvn -pl cn.edu.ruc:"$MVN_PROJECT" -am package -DskipTests
	  cd build
	  echo pwd
      if [ "$?" -ne 0 ] ; then
        echo "[ERROR] Error trying to build project. Exiting."
        exit 1;
      fi
    fi

  done

  # Core libraries
  for f in "$RUCTS_HOME"/bm-core/target/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done
  for f in "$RUCTS_HOME"/bm-core/target/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done
fi 
echo "===================================================="
echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH $RUCTS_CLASS"
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "
$CLASSPATH" $RUCTS_CLASS
