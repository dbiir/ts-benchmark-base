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
# Set start type import or perform
START_TYPE=$1
if [ "$START_TYPE" = "import" ];then
STARUP="load.online"
fi

if [ "$START_TYPE" = "generator" ];then
STARUP="load.offline"
fi
if [ "$START_TYPE" = "perform" ];then
STARUP="perform"
fi
if [ "$START_TYPE" = "sap" ];then
STARUP="sap"
fi
if [ "$START_TYPE" = "as2s" ];then
STARUP="as2s"
fi
echo $STARUP
echo $START_TYPE
echo "==================sssss============="
# Only set RUCTS_HOME if not already set
[ -z "$RUCTS_HOME" ] && RUCTS_HOME=$(cd "$SCRIPT_DIR/.." || exit; pwd)
BINDING_LINE=$(grep "^$2:" "$RUCTS_HOME/build/bindings.properties" -m 1)

BINDING_NAME=$(echo "$BINDING_LINE" | cut -d':' -f1)
BINDING_CLASS=$(echo "$BINDING_LINE" | cut -d':' -f2)
echo $BINDING_NAME
echo $BINDING_CLASS 
# Check if source checkout, or release distribution
DISTRIBUTION=true
if [ -r "$RUCTS_HOME/pom.xml" ]; then
  DISTRIBUTION=false;
fi


RUCTS_CLASS='cn.edu.ruc.biz.Core'
BINDING_DIR=$(echo "$BINDING_NAME" | cut -d'-' -f1)
BINDING_DIR="bm-"$BINDING_DIR
echo "=========================================="
# Build classpath
#   The "if" check after the "for" is because glob may just return the pattern
#   when no files are found.  The "if" makes sure the file is really there.
if $DISTRIBUTION; then
  # Core libraries
  for f in "$RUCTS_HOME"/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

  # Database conf dir
  if [ -r "$RUCTS_HOME"/"$BINDING_DIR"/conf ] ; then
    CLASSPATH="$CLASSPATH:$RUCTS_HOME/$BINDING_DIR/conf"
  fi

  # Database libraries
  for f in "$RUCTS_HOME"/"$BINDING_DIR"/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

# Source checkout
else
  # Check for some basic libraries to see if the source has been built.
  for f in "$RUCTS_HOME"/"$BINDING_DIR"/target/*.jar ; do

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

  # Database conf (need to find because location is not consistent)
  CLASSPATH_CONF=$(find "$RUCTS_HOME"/$BINDING_DIR -name "conf" | while IFS="" read -r file; do echo ":$file"; done)
  if [ "x$CLASSPATH_CONF" != "x" ]; then
    CLASSPATH="$CLASSPATH$CLASSPATH_CONF"
  fi


  # Database libraries
  for f in "$RUCTS_HOME"/"$BINDING_DIR"/target/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done

  # Database dependency libraries
  for f in "$RUCTS_HOME"/"$BINDING_DIR"/target/lib/*.jar ; do
    if [ -r "$f" ] ; then
      CLASSPATH="$CLASSPATH:$f"
    fi
  done
fi

# Get the rest of the arguments
RUCTS_ARGS=$(echo "$@" | cut -d' ' -f3-)

echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH $RUCTS_CLASS $RUCTS_COMMAND -db $BINDING_CLASS $RUCTS_ARGS"
echo "===================================================="
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "$CLASSPATH" $RUCTS_CLASS $RUCTS_COMMAND -db $BINDING_CLASS -starup $STARUP  $RUCTS_ARGS
echo "===================================================="

