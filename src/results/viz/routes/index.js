// settings
var express = require("express");
var fs = require("fs");
var router = express.Router();
var baseUrl = "/rest";

// static data
router.get(baseUrl + "/:filename", function(req, res) {
    
    // get URL params
    var name = req.params.filename;
    
    // read static file
    fs.readFile("./routes/data/" + name + ".json", function(err, data) {
        
        // check for error
        if (err) throw err;
        
        // espose in service
        res.send(data);
        
    });
        
});

module.exports = router;