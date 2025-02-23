build:
	docker compose build

down:
	docker compose down --volumes --remove-orphans

run:
	make down && docker compose up

run-scaled:
	make down && docker compose up --scale spark-worker=3

stop:
	docker compose stop

submit:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

submit-iceberg:
	docker exec da-spark-master spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0 --jars /opt/spark/jars/iceberg-spark-runtime.jar --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)




