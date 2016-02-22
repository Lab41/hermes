// settings
var express = require("express");
var fs = require("fs");
var readline = require("readline");
var router = express.Router();
var baseUrl = "/rest";

// static data
router.get(baseUrl + "/:filename", function(req, res) {
    
    // get URL params
    var name = req.params.filename;
	var ext = req.query.ext;
    var resultsFolder = __dirname + "/../../" + name + "." + ext;
	
    // read static file
    fs.readFile(resultsFolder, function(err, data) {
        
        // check for error
        if (err) throw err;
		
		// check for csv
		if (ext == "csv") {
			
			// get structure
			var lines = data.toString().split("\r");
			var columns = lines[0].split(",");
			var json = [];
			
			// loop through lines
			for (var i=1; i < lines.length; i++) {
				
				// create obj for each line
				var lineObj = {};
				
				var lineValues = lines[i].split(",");
				
				// loop through columns
				for (var c=0; c < columns.length; c++) {
					
					var columnName = columns[c];
					
					// add data to obj
					lineObj[columnName] = lineValues[c];
						
				};
				
				// add obj to json
				json.push(lineObj);
				
			};
			
			data = json;
			
		};
        
        // espose in service
        res.send(data);
        
    });
        
});

// nested data
router.get(baseUrl + "/nest/:filename", function(req, res) {
    
    // get URL params
    var name = req.params.filename;
	var ext = req.query.ext;
    var resultsFolder = __dirname + "/../../" + name + "." + ext;
	
    // read static file
    fs.readFile(resultsFolder, function(err, data) {
        
        // check for error
        if (err) throw err;
		
		// check for csv
		if (ext == "csv") {
			
			// get structure
			var lines = data.toString().split("\r");
			var columns = lines[0].split(",");
			var json = [];
            var algIdx = 5;
            var algs = []; // check which algorithms already have an object
			
			// loop through lines
			for (var i=1; i < lines.length; i++) {
                
                var lineValues = lines[i].split(",");
                var algName = lineValues[algIdx];
                
                // check algorithm array
                var algExists = algs.indexOf(algName);
               
                // new algorithm object needed
                if (algExists == -1) {
                    
                    // create obj for each line
				    var algObj = { name: algName, values: ["test"] };
                    
                    // add obj to json
                    json.push(algObj);
                    
                    // track objs
                    algs.push(algName);
                    
                } else {
                    
                    // algorithm object exists
                    var algObj = json[algExists];
                    
                    // add to values
                    algObj.values.push("test");
                    
                }
				
				/*
				
				// loop through columns
				for (var c=0; c < columns.length; c++) {
					
					var columnName = columns[c];
					
					// add data to obj
					lineObj[columnName] = lineValues[c];
						
				};*/
				
			};
			
			data = json;
			
		};
        
        // espose in service
        res.send(data);
        
    });
        
});

module.exports = router;