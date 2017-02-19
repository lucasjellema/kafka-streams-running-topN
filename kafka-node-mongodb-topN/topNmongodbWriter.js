/*

Simple application that connects to MongoDB (using the mongodb module ) and then updates two documents in the top3 collection in the test database; if a document does not yet exist (based on the key which is the continent property) it is inserted.
When the application is done running, two documents exist (and have their lastModified property set if they were updated). 

*/

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');

// connect string for mongodb server running locally, connecting to a database called test
var url = 'mongodb://127.0.0.1:27017/test';

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected correctly to server.");
   var doc = {
        "continent" : "Europe",
         "nrs" : [ {"name":"Belgium"}, {"name":"Luxemburg"}]
      };
   var doc2 = {
        "continent" : "Asia",
         "nrs" : [ {"name":"China"}, {"name":"India"}]
      };
  insertDocument(db,doc, function() {
    console.log("returned from processing doc "+doc.continent);  
    insertDocument(db,doc2, function() {
      console.log("returned from processing doc "+doc2.continent);          
      db.close();
      console.log("Connection to database is closed. Two documents should exist, either just created or updated. ");
      console.log("From the MongoDB shell: db.top3.find() should list the documents. ");
    });
  });
});

var insertDocument = function(db, doc, callback) {
   // first try to update; if a document could be updated, we're done 
   console.log("Processing doc for "+doc.continent);
   updateTop3ForContinent( db, doc, function (results) {      
       if (!results || results.result.n == 0) {
          // the document was not updated so presumably it does not exist; let's insert it  
          db.collection('top3').insertOne( 
                doc
              , function(err, result) {
                   assert.equal(err, null);
                   callback();
                }
              );   
       }//if
       else {
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
