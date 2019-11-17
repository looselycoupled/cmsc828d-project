#!/bin/bash


function checkYN {
  while true
  do
    read -p "$1 - Abort/No/Yes " action
    case $action in
      a|A|abort)
        echo "Aborting"
        exit 1
        ;;
      n|N|no)
        return 1
        ;;
      y|Y|yes)
        return 0
        ;;
      *)
        echo "type A, N or Y"
        ;;
      esac
  done
}

function bail {
  echo "Subcommand failed"
  exit 1
}


echo
echo "setup.sh should only be run at the root of this project/repo"
echo
if ! checkYN "is the current working directory correct?"; then
  echo "well, you should probably change directories then"
  exit 1
fi

export AIRFLOW_HOME=$(pwd)/airflow
echo
echo setting environment variables
echo AIRFLOW_HOME=$AIRFLOW_HOME
echo

if checkYN "install python depenencies?"; then
    pip install -r requirements.txt
fi
