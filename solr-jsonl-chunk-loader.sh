#!/usr/bin/env bash
set -e

scriptDir=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $scriptDir/common_routines.sh

require_env_var INPUT_JSONL
require_env_var SOLR_COLLECTION
require_env_var SCHEMA_VERSION

COLLECTION=${SOLR_COLLECTION}-v${SCHEMA_VERSION}
# Space separated list of hosts
SOLR_HOST=${SOLR_HOST:-'localhost:8983'}
SOLR_HOSTS=${SOLR_HOSTS:-${SOLR_HOST}}
SOLR_USER=${SOLR_USER:-"solr"}
SOLR_PASS=${SOLR_PASS:-"SolrRocks"}
SOLR_AUTH="-u $SOLR_USER:$SOLR_PASS"
# SOLR_PROCESSORS must be null or a comma-separated list of processors to use during an update
if [[ $SOLR_PROCESSORS ]]
then
  PROCESSOR="?processor=${SOLR_PROCESSORS}"
fi

# Creates a new file descriptor 3 that redirects to 1 (STDOUT) to see curl progress and also to cleanly capture http_code, see commit and post_json below
exec 3>&1

SOLR_HOSTS_ARR=(${SOLR_HOSTS})

commit() {
  echo "Committing files on ${1}..."
  HTTP_STATUS=$(curl $SOLR_AUTH -o >(cat >&3) -w "%{http_code}" "http://${1}/solr/${COLLECTION}/update" --data-binary '{ "commit": {} }' -H 'Content-type:application/json')

  if [[ ! ${HTTP_STATUS} == 2* ]]
  then
    echo "Error during commit!" && exit 1
  fi
}

post_json() {
  local MAX_RETRIES=10
  local RETRIES=0
  local HTTP_STATUS=0
  WAIT_TIME=10
  while [[ ! ${HTTP_STATUS} == 2* ]] && [[ ${RETRIES} -lt ${MAX_RETRIES} ]]
  do
    # Run curl in a separate command, capturing output of -w "%{http_code}" into HTTP_STATUS
    # and sending the content to this command's STDOUT with -o >(cat >&3)
    # The update/json/docs handler supports both regular JSON and JSON Lines:
    # https://solr.apache.org/guide/8_7/transforming-and-indexing-custom-json.html#multiple-documents-in-a-single-payload
    HTTP_STATUS=$(curl ${SOLR_AUTH} -o >(cat >&3) -w "%{http_code}" "http://${1}/solr/${COLLECTION}/update/json/docs${PROCESSOR}" --data-binary "@${2}" -H 'Content-type:application/json')
    if [[ ! ${HTTP_STATUS} == 2* ]]
    then
      echo "Warning: something went wrong when sending the JSONL file to ${1}; retrying in ${WAIT_TIME} seconds..."
      sleep ${WAIT_TIME}
      RETRIES=$((RETRIES + 1))
      WAIT_TIME=$((WAIT_TIME + WAIT_TIME))
    fi
  done

  if [[ ! ${HTTP_STATUS} == 2* ]] || [[ ${RETRIES} -eq ${MAX_RETRIES} ]]
  then
    echo "Error: something went wrong; please, check the messages above and the logs in Solr (I tried ${MAX_RETRIES} times and gave up!)"
  fi
}


COMMIT_DOCS=${SOLR_COMMIT_DOCS:-1000000}
echo "Loading $INPUT_JSONL into hosts ${SOLR_HOSTS_ARR[@]} collection $COLLECTION committing every ${COMMIT_DOCS} docs..."

CHUNK_PREFIX=${CHUNK_PREFIX:-`basename -s .jsonl ${INPUT_JSONL}`-}

NUM_DOCS_PER_BATCH=${NUM_DOCS_PER_BATCH:-50000}
split -a 3 -l $NUM_DOCS_PER_BATCH $INPUT_JSONL $CHUNK_PREFIX --additional-suffix .jsonl
CHUNK_FILES=$(ls $CHUNK_PREFIX*)

cleanup() {
  exec 3>&-
  rm ${CHUNK_FILES}
}


trap cleanup exit
# I is used to print the progress of a chunk file (e.g. 1/10)
# J is used to round-robin the hosts
I=0
J=0
for CHUNK_FILE in $CHUNK_FILES
do
  SOLR_HOST=${SOLR_HOSTS_ARR[(( ${J} % ${#SOLR_HOSTS_ARR[@]} ))]}
  J=$(( $J + 1 ))

  I=$(( $I + 1 ))
  echo "$CHUNK_FILE ${I}/$(wc -w <<< $CHUNK_FILES) -> ${SOLR_HOST}"

  post_json ${SOLR_HOST} ${CHUNK_FILE}

  if [[ $(( $I % ( $COMMIT_DOCS / $NUM_DOCS_PER_BATCH) )) == 0 ]]
  then
    # Make the commit in the next host
    J=$(( $J + 1 ))
    SOLR_HOST=${SOLR_HOSTS_ARR[(( ${J} % ${#SOLR_HOSTS_ARR[@]} ))]}
    commit ${SOLR_HOST}
  fi
done

J=$(( $J + 1 ))
SOLR_HOST=${SOLR_HOSTS_ARR[(( ${J} % ${#SOLR_HOSTS_ARR[@]} ))]}
commit ${SOLR_HOST}
