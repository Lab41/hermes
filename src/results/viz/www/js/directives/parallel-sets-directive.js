angular.module("parallel-sets-directive", [])

.directive("parallelSets", ["d3Service", function(d3Service){
    return {
        restrict: "E",
        scope: {
            vizData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            colorRange: "="
        },
        template: "<p>parallel sets</p>",
        link: function(scope, element, attrs){
            
            //get d3 promise
            d3Service.d3().then(function(d3) {
				
				/////////////////////////////////////////
                ////// values from html attributes //////
                /////////////////////////////////////////
                
                // set sizes from attributes in html element
                // if not attributes present - use default
                var width = parseInt(attrs.canvasWidth) || 400;
                var height = parseInt(attrs.canvasHeight) || 200;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
				
				///////////////////////////////////
                ////// basic layout settings //////
                ///////////////////////////////////
				
				var padding = { bottom: 20, left: 10, right: 10, top: 20 };
				var activeWidth = width - (padding.left + padding.right);
				var activeHeight = height - (padding.bottom + padding.top);
				
				///////////////////////////////////////////////////////
                ////// main svg constructs not dependent on data //////
                ///////////////////////////////////////////////////////
                                
                // create svg canvas
                var canvas = d3.select(element[0])
                    .append("svg")
                    .attr({
                        viewBox: "0 0 " + width + " " + height
                    })
					.append("g")
					.attr({
						transform: "translate(" + padding.left + "," + padding.top + ")"
					});
                
                // scales
				var xScale = d3.scale.ordinal()
                    .rangePoints([0, activeWidth], 1);
                
                var y = {};
                
                var line = d3.svg.line();
				var axis = d3.svg.axis()
					.orient("left");
				var background;
				var foreground;
				
				///////////////////////////////////////////////
                ////// dynamic d3 runs every data update //////
                ///////////////////////////////////////////////
												
                // check for new data
                scope.$watch("vizData", function(newData, oldData) {
                    
                    // async check
                    if (newData !== undefined) {
                    
                        // check new vs old
                        var isMatching = angular.equals(newData, oldData);

                        // if false
                        //if (!isMatching) {

                            // update the viz
                            draw(newData);

                        //};
                        
                        function draw(data) {
                            
                            var dataNew = [];
                            
                            // format TODO server-side
                            angular.forEach(data, function(value, key) {
                                
                                var keys = Object.keys(value);
                                var obj = {}
                                
                                // loop through keys
                                for (var i=0; i < keys.length; i++) {
                                    
                                    var currentKey = keys[i];
                                    var floatVal = parseFloat(value[currentKey]);
                                    
                                    // check if float
                                    if (floatVal != 100 && !isNaN(floatVal) && currentKey != "Column1") {
                                        
                                        // add to new obj
                                        obj[currentKey] = floatVal;
                                        
                                    };
                                    
                                    // add to arry
                                    dataNew.push(obj);
                                    
                                };
                                
                            });
							
							var data = dataNew;
							
							// Extract the list of dimensions and create a scale for each
							xScale.domain(dimensions = d3.keys(data[0]).filter(function(d) {
								return d != "name" && (y[d] = d3.scale.linear()
													   .domain(d3.extent(data, function(p) { return +p[d]; }))
													   .range([height, 0]));
							}));

  // Add grey background lines for context.
  /*background = canvas.append("g")
      .attr("class", "background")
    .selectAll("path")
      .data(data)
    .enter().append("path")
      .attr("d", path);*/

  // Add blue foreground lines for focus.
  foreground = canvas.append("g")
      .attr("class", "foreground")
    .selectAll("path")
      .data(data)
    .enter().append("path")
      .attr("d", path);

  // Add a group element for each dimension.
  /*var g = canvas.selectAll(".dimension")
      .data(dimensions)
    .enter().append("g")
      .attr("class", "dimension")
      .attr("transform", function(d) { return "translate(" + xScale(d) + ")"; });

  // Add an axis and title.
  /*g.append("g")
      .attr("class", "axis")
      .each(function(d) { d3.select(this).call(axis.scale(y[d])); })
    .append("text")
      .style("text-anchor", "middle")
      .attr("y", -9)
      .text(function(d) { return d; });*/

  // Add and store a brush for each axis.
  /*g.append("g")
      .attr("class", "brush")
      .each(function(d) { d3.select(this).call(y[d].brush = d3.svg.brush().y(y[d]).on("brush", brush)); })
    .selectAll("rect")
      .attr("x", -8)
      .attr("width", 16);*/

// Returns the path for a given data point.
function path(d) {
  return line(dimensions.map(function(p) { return [xScale(p), y[p](d[p])]; }));
}

// Handles a brush event, toggling the display of foreground lines.
/*function brush() {
  var actives = dimensions.filter(function(p) { return !y[p].brush.empty(); }),
      extents = actives.map(function(p) { return y[p].brush.extent(); });
  foreground.style("display", function(d) {
    return actives.every(function(p, i) {
      return extents[i][0] <= d[p] && d[p] <= extents[i][1];
    }) ? null : "none";
  });
}*/

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);