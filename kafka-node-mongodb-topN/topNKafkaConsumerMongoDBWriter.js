/*
This program connects to MongoDB (using the mongodb module )
This program consumes Kafka messages from topic Top3CountrySizePerContinent to which the Running Top3 (size of countries by continent) is produced.

This program records each latest update of the top 3 largest countries for a continent in MongoDB. If a document does not yet exist for a continent (based on the key which is the continent property) it is inserted.

The program ensures that the MongoDB /test/top3 collection contains the latest Top 3 for each continent at any point in time.

*/

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');

var kafka = require('kafka-node')
var Consumer = kafka.Consumer
var client = new kafka.Client("ubuntu:2181/")
var countriesTopic = "Top3CountrySizePerContinent";


// connect string for mongodb server running locally, connecting to a database called test
var url = 'mongodb://127.0.0.1:27017/test';
var mongodb;

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected correctly to MongoDB server.");
  mongodb = db;
});

var insertDocument = function(db, doc, callback) {
   // first try to update; if a document could be updated, we're done 
   updateTop3ForContinent( db, doc, function (results) {      
       if (!results || results.result.n == 0) {
          // the document was not updated so presumably it does not exist; let's insert it  
          db.collection('top3').insertOne( 
                doc
              , function(err, result) {
                   assert.equal(err, null);
                   console.log("Inserted doc for "+doc.continent);
                   callback();
                }
              );   
       }//if
       else {
         console.log("Updated doc for "+doc.continent);
         callback();
       }
 }); //updateTop3ForContinent
}; //insertDocument

var updateTop3ForContinent = function(db, top3 , callback) {
   db.collection('top3').updateOne(
      { "continent" : top3.continent },
      {
        $set: { "nrs": top3.nrs },
        $currentDate: { "lastModified": true }
      }, function(err, results) {
      //console.log(results);
      callback(results);
   });
};

// Configure Kafka Consumer for Kafka Top3 Topic and handle Kafka message (by calling updateSseClients)
var consumer = new Consumer(
  client,
  [],
  {fromOffset: true}
);

consumer.on('message', function (message) {
  handleCountryMessage(message);
});

consumer.addTopics([
  { topic: countriesTopic, partition: 0, offset: 0}
], () => console.log("topic "+countriesTopic+" added to consumer for listening"));

function handleCountryMessage(countryMessage) {
    var top3 = JSON.parse(countryMessage.value);
    var continent = new Buffer(countryMessage.key).toString('ascii');
    top3.continent = continent;
    // insert or update the top3 in the MongoDB server
    insertDocument(mongodb,top3, function() {
      console.log("Top3 recorded in MongoDB for "+top3.continent);  
    });

}// handleCountryMessage
