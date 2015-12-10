case $1 in
start)
	echo "--- now start the hadoop ---"
	start-dfs.sh
	start-yarn.sh
	echo "--- done ---"
	;;
stop)
	echo "--- now stop the hadoop ---"
	stop-dfs.sh
	stop-yarn.sh
	echo "--- done ---"
	;;
esac
