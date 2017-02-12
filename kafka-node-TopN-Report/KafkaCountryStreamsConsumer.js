/*

This program consumes Kafka messages from topic Top3CountrySizePerContinent to which the Running Top3 (size of countries by continent) is produced.

This program reports: top 3 largest countries per continent (periodically, with a configurable interval) 
*/


var kafka = require('kafka-node')
var Consumer = kafka.Consumer
var client = new kafka.Client("ubuntu:2181/")

var countriesTopic = "Top3CountrySizePerContinent";
var reportingIntervalInSecs = 4;

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

var countrySizeStandings = {}; // the global container for the most recent country size standings 

function handleCountryMessage(countryMessage) {
    var top3 = JSON.parse(countryMessage.value);
    var continent = new Buffer(countryMessage.key).toString('ascii');
    //console.log ("key "+continent);
    countrySizeStandings[continent]=top3;
}// handleCountryMessage

// every reportingIntervalInSecs seconds, report on the current standings per continent
function report() {
   var d = new Date();
   console.log("Report at "+ d.getHours()+":"+d.getMinutes()+ ":"+d.getSeconds());
   // loop over the keys (properties) in the countrySizeStandings map (object)
   for (var continent in countrySizeStandings) {
     if (countrySizeStandings.hasOwnProperty(continent)) {
        // do stuff
        //console.log("Present standings for continent "+continent);
        var line = continent+ ": ";
        var index = 1;
        countrySizeStandings[continent].nrs.forEach(function(c) {
          if (c) {
            line = line + (index++) +'. '+ c.name+ '('+c.size+'), ';
          }
        });
        console.log(line);
    }//if
  }//for
}//report

// schedule execution of function report at the indicated interval
setInterval(report, reportingIntervalInSecs*1000);