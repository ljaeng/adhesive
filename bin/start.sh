

CLUSTER="zjyprc-hadoop"

#spark job
/home/rd/tools/infra-client/bin/spark-submit --cluster ${CLUSTER}-spark2.1 --force-update \
--master yarn \
--deploy-mode yarn-client \
--queue root.production.miui_group.weather.preload \
--driver-memory 2g \
--executor-memory 3g \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=700 \
--conf spark.dynamicAllocation.executorIdleTimeout=600s \
--conf spark.speculation=true \
--conf spark.speculation.multiplier=2 \
--conf spark.speculation.quantile=0.5 \
--conf spark.scheduler.executorTaskBlacklistTime=300000 \
--conf spark.blacklist.enabled=true \
--conf spark.shuffle.compress=false \
--conf spark.shuffle.spill.compress=false  \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.yarn.job.owners=lizheng \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/rd/developer/lizheng/adhesive/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/rd/developer/lizheng/adhesive/log4j.properties" \
--class  com.jaeng.adhesive.core.Launcher \
/home/rd/developer/lizheng/adhesive/adhesive-job-1.0-SNAPSHOT.jar \
-mode line
