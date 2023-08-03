export DOCKER_IMAGE_NAME=test
export CURRENT_DIR=$(pwd)

docker run --interactive --tty --volume `pwd`/src:/src \
		--volume ${CURRENT_DIR}/datalake-test/:/datalake \
		${DOCKER_IMAGE_NAME} bash '-c' "cd /home && export PYTHONIOENCODING=utf8 && spark-submit \
		/src/task2.py \
		--partitions-output "4" \
		1> >(sed $$'s,.*,\e[32m&\e[m,' >&2)" || true