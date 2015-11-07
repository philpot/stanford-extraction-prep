/usr/lib/spark/bin/spark-submit \
--master yarn-client \
--driver-memory 8g \
--executor-memory 80G  --executor-cores 5 \
--num-executors 20 \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  prep.py \
  $@

