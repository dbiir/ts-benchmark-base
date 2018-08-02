#! /bin/bash


OS=$(uname -a | cut -d " " -f 1)
if [[ "$OS" = "Darwin" ]]; then
	echo "this os is macos "
elif [[ "$OS" = "Linux" ]]; then
	#statements
	echo "this os is linux"
fi
# sed -i "" "s/shan/hua/g"  routine

#组实验配置文件名,文件中一行对应一次实验的一个变化参数如LOOP=10,注意等号两边不能有空格
FILENAME=routine/routine_read

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

#cat $BENCHMARK_HOME/$FILENAME | while read LINE
FILE=$(cat $BENCHMARK_HOME/$FILENAME)
#echo $FILE
#实际上LINE是以换行或空格为分隔符
for LINE in $FILE;
do
  CHANGE_PARAMETER=$(echo $LINE | cut -d = -f 1)
  if [ -n "$LINE" ]; then
    if [ "$LINE" != "TEST" ]; then
    	if [[ "$OS" = "Darwin" ]]; then
			sed -i "" "s/^${CHANGE_PARAMETER}.*$/${LINE}/g" $BENCHMARK_HOME/conf/cfg_dyn.properties
		elif [[ "$OS" = "Linux" ]]; then
			#statements
			sed -i "s/^${CHANGE_PARAMETER}.*$/${LINE}/g" $BENCHMARK_HOME/conf/cfg_dyn.properties
		fi
        grep $CHANGE_PARAMETER  $BENCHMARK_HOME/conf/cfg_dyn.properties
    else
        sh $BENCHMARK_HOME/tsbm_dyn.sh
        sleep 2
    fi
  fi
done