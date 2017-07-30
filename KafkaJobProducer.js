/*
This program reads and parses all lines from csv files into an array (messageArray) of arrays; each nested array represents a msg.
The initial file read is synchronous. The msg records are kept in memory.
After the the initial read is performed, a function is invoked to publish a message to Kafka for the first msg in the array. This function then uses a time out with a random delay
to schedule itself to process the next msg record in the same way. Depending on how the delays pan out, this program will publish msg messages to Kafka every 3 seconds for about 10 minutes.
*/

var fs = require('fs');
var parse = require('csv-parse');
var dt = require('node-datetime');
var _ = require('underscore');

// Kafka configuration
var kafka = require('kafka-node')
var Producer = kafka.Producer
// instantiate client with as connectstring host:port for  the ZooKeeper for the Kafka cluster
var client = new kafka.Client("localhost:32181/")

// name of the topic to produce to
var kafkaTopic = process.argv[2] || "jobStatus",
    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    kafkaProducerReady = false ;

producer.on('ready', function () {
    console.log("Producer ready");
    kafkaProducerReady = true;
});

producer.on('error', function (err) {
    console.error("Problem with producing Kafka message "+err);
});

var inputFile='job_status.csv';
var averageDelay = 1000;  // in miliseconds
var spreadInDelay = 1000; // in miliseconds

var messageArray ;

var parser = parse({delimiter: ','}, function (err, data) {
    messageArray = data;
    // when all messages are available,then process the first one
    // note: array element at index 0 contains the row of headers that we should skip
    handleMessage(1, 0);
});

// read the inputFile, feed the contents to the parser
fs.createReadStream(inputFile).pipe(parser);

var payloads = [];

// handle the current input message
function handleMessage( currentRecord, headerRecord) {
    if (currentRecord < messageArray.length) {
        var line = messageArray[currentRecord],
            header = messageArray[headerRecord];
        var msg = {};
        for (var i = 0; i < header.length; i++) {
            msg[header[i].trim()] = line[i];
        };
        msg['_msgTimestamp'] = dt.create().format('YmdHMSN');
        console.log(JSON.stringify(msg));
        // produce message to Kafka
        produceMessage(msg);

        // schedule this function to process next msg after a random delay of between averageDelay plus or minus spreadInDelay )
        var delay = averageDelay + (Math.random() - 0.5) * spreadInDelay;
        //note: use bind to pass in the value for the input parameter currentRecord
        setTimeout(handleMessage.bind(null, currentRecord + 1, headerRecord), delay);
    }
    else {
        // We've done all the entries.
        process.exit();
    }
}//handleMessage

function produceMessage(msg) {
    var KeyedMessage = kafka.KeyedMessage;
    var msgKM = new KeyedMessage(msg.code, JSON.stringify(msg));
    var newMsg = _.range(2).map(function () { return msgKM });

    payloads.push({ topic: 'jobStatus', messages: newMsg, partition: 0 });
    payloads.push({ topic: 'jobStatusX', messages: newMsg, partition: 0 });

    if (kafkaProducerReady) {
        producer.send(payloads, function (err, data) {
            if (err){
                console.log("ERROR: send error " + err.toString());
            } else {
                console.log("Message sent");
            }
            // TODO: Should do error checking here first
            payloads = [];
        });
    } else {
        console.error("Producer is not ready yet, message queued.");
    }
}//produceMessage

