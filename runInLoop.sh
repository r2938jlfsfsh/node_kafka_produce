while [[ 1 -eq 1 ]]; do
  node ./KafkaJobProducer.js
  sleep ${1:-5}
done
