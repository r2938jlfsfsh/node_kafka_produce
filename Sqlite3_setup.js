/*
This program reads and parses all lines from csv files into an array (messageArray) of arrays; each nested array represents a msg.
The initial file read is synchronous. The msg records are kept in memory.
After the the initial read is performed, a function is invoked to publish a message to Kafka for the first msg in the array. This function then uses a time out with a random delay
to schedule itself to process the next msg record in the same way. Depending on how the delays pan out, this program will publish msg messages to Kafka every 3 seconds for about 10 minutes.
*/

var fs = require('fs');
var parse = require('csv-parse');
var dt = require('node-datetime');

var inputFile='job_status_init.csv';
var dbFilename = '/home/rd/WebstormProjects/db/sqllite.db';
var averageDelay = 3;  // in miliseconds
var spreadInDelay = 1; // in miliseconds

var messageArray ;

var parser = parse({delimiter: ','}, function (err, data) {
    messageArray = data;
    // when all messages are available,then process the first one
    // note: array element at index 0 contains the row of headers that we should skip
    handleMessage(1, 0);
});

var sqlite3 = require('sqlite3').verbose();
try{
    fs.unlinkSync(dbFilename);
    console.log("Deleted existing file");
} catch(err){
    console.log("Failed to delete existing file");
}

var db = new sqlite3.Database(dbFilename);

db.serialize(function() {
    db.run("CREATE TABLE job_status (job_name text,source_system text,status text," +
        " start_time integer, end_time integer, last_update integer)");

    db.run("CREATE VIEW IF NOT EXISTS job_status_vw AS " +
        "SELECT job_name, source_system, status, datetime(start_time, 'unixepoch', 'localtime') as start_time, " +
        "datetime(end_time, 'unixepoch', 'localtime') as end_time, datetime(last_update, 'unixepoch', 'localtime') as _msgTimestamp" +
        " FROM job_status");

    var stmt = db.prepare("INSERT INTO job_status (job_name,source_system,last_update) " +
        "VALUES (?,?,?)");
    for (var i = 0; i < 10000; i=i+2) {
        console.log("insert " + i + ", " + Math.floor(new Date() / 1000));
        stmt.run("job" + i, 'CAP',Math.floor(new Date() / 1000));
    }
    stmt.finalize();

/*    db.each("SELECT job_name, source_system, status, start_time, end_time, last_update, datetime(last_update, 'unixepoch', 'localtime') as last_update_dt" +
        " FROM job_status"
        , function(err, row) {
        //console.log(row.id + ": " + row.info);
            console.log(JSON.stringify(row));
    });
    */
    db.each("SELECT * FROM job_status_vw"
        , function(err, row) {
            //console.log(row.id + ": " + row.info);
            console.log(JSON.stringify(row));
        });
});

db.close();

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
    payloads.push({ topic: kafkaTopic, messages: msgKM, partition: 0 });

    if (kafkaProducerReady) {
        producer.send(payloads, function (err, data) {
            console.log(data);
            // TODO: Should do error checking here first
            payloads = [];
        });
    } else {
        console.error("Producer is not ready yet, message queued.");
    }
}//produceMessage

// read the inputFile, feed the contents to the parser
//fs.createReadStream(inputFile).pipe(parser);

