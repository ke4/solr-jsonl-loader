#!/usr/bin/env bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

require_var_opt() {
  if [[ -z ${!1} ]]
  then
    echo "${2} option is mandatory."
    exit 1
  fi
}

function print_usage() {
  printf '\n%b\n' "Usage: ${0} -i FILE -c COLLECTION [ -s SOLR_HOSTS] [ -b NUMBER] [ -n NUMBER ] [ -u USER ] [ -p PASSWORD ] [ -r PROCESSORS ]"
  printf '\n%b\n' "One liner explanation."

  printf '\n%b\n' "-i\t\t "
  printf '\n%b\n' "-c FILE \t "
  printf '%b\n\n' "-s SOLR_HOSTS \t "
  printf '%b\n\n' "-b BATCH_SIZE \t "
  printf '%b\n\n' "-n NUMBER \t "
  printf '%b\n\n' "-u USER \t "
  printf '%b\n\n' "-p PASSWORD \t "
  printf '%b\n\n' "-r PROCESSORS \t Comma-separated list of processors to use during an update"
  printf '%b\n\n' "-h \t\t "
}

SOLR_HOSTS_ARR=("localhost:8983")
NUM_DOCS_PER_BATCH=50000
COMMIT_DOCS=1000000
SOLR_USER="solr"
SOLR_PASS="SolrRocks"
while getopts "i:c:s:b:n:u:p:r:h" opt
do
  case ${opt} in
    i)
      INPUT_JSONL=${OPTARG}
      ;;
    c)
      COLLECTION=${OPTARG}
      ;;
    s)
      # Space separated list of hosts
      # Create an array from the space-separated list of hosts
      SOLR_HOSTS_ARR=(${OPTARG})
      ;;
    n)
      COMMIT_DOCS=${OPTARG}
      ;;
    b)
      NUM_DOCS_PER_BATCH=${OPTARG}
      ;;
    u)
      SOLR_USER=${OPTARG}
      ;;
    p)
      SOLR_PASS=${OPTARG}
      ;;
    r)
      SOLR_PROCESSORS=${OPTARG}
      ;;
    h)
      print_usage
      exit 0
      ;;
    \?)
      printf '%b\n' "Invalid option: -${OPTARG}" >&2
      print_usage
      exit 2
      ;;
  esac
done

require_var_opt "INPUT_JSONL" "-i"
require_var_opt "COLLECTION" "-c"

SOLR_AUTH="-u ${SOLR_USER}:${SOLR_PASS}"

if [[ ${SOLR_PROCESSORS} ]]
then
  PROCESSOR="?processor=${SOLR_PROCESSORS}"
fi

echo "Loading ${INPUT_JSONL} into hosts ${SOLR_HOSTS_ARR[@]}, collection ${COLLECTION}, committing every ${COMMIT_DOCS} docs..."

CHUNK_PREFIX=${CHUNK_PREFIX:-`basename -s .jsonl ${INPUT_JSONL}`-}
split -a 3 -l ${NUM_DOCS_PER_BATCH} ${INPUT_JSONL} ${CHUNK_PREFIX} --additional-suffix .jsonl
CHUNK_FILES=$(ls ${CHUNK_PREFIX}*)

# Create a new file descriptor 3 that redirects to 1 (STDOUT) to see curl progress and also to capture http_code;
# See commit() and post_json() below.
exec 3>&1

commit() {
  echo "Commit docs -> ${1}"
  HTTP_STATUS=$(curl ${SOLR_AUTH} -o >(cat >&3) -w "%{http_code}" "http://${1}/solr/${COLLECTION}/update" --data-binary '{ "commit": {} }' -H 'Content-type:application/json')

  if [[ ! ${HTTP_STATUS} == 2* ]]
  then
    echo "Error: something went wrong during commit; please, check the messages above and the logs in Solr"
  fi
}

post_json() {
  # Run curl in a separate command, capturing output of -w "%{http_code}" into HTTP_STATUS
  # and sending the content to this command's STDOUT with -o >(cat >&3)
  local MAX_RETRIES=10
  local RETRIES=0
  local HTTP_STATUS=0
  WAIT_TIME=5
  while [[ ! ${HTTP_STATUS} == 2* ]] && [[ ${RETRIES} -lt ${MAX_RETRIES} ]]
  do
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

cleanup() {
  exec 3>&-
  rm ${CHUNK_FILES}
}
trap cleanup exit

# I is used to print the progress of a chunk file (e.g. 1/10)
# J is used to round-robin the Solr hosts
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
