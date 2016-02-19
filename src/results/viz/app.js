var express = require('express');
var app = express();

// backend API
app.use(require("./routes/index"));

// static front end
app.use("/", express.static(__dirname + "/www"));

var server = app.listen(3111, 'localhost', function () {
  var host = server.address().address;
  var port = server.address().port;

  console.log('app listening at http://%s:%s', host, port);
    
});

module.exports = app;