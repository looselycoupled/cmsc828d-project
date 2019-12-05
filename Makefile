resetdb:
	psql -c "drop database airflow" || true
	psql -c "create database airflow"
	airflow initdb

.PONY: resetdb
