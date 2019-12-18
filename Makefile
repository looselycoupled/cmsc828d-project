clearlogs:
	rm -rf airflow/logs/*

resetpg:
	psql -c "drop database airflow" || true
	psql -c "create database airflow"
	airflow initdb

resetmongo:
	mongo ariadne --eval "printjson(db.dropDatabase())"

resetall: clearlogs resetpg resetmongo


.PHONY: resetall clearlogs resetpg resetmongo
