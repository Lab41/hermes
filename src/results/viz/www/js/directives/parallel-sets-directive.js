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
                    .rangePoints([padding.left, activeWidth]);
                
                var cScale = d3.scale.ordinal()
                    .range(colorRange);
                
                var yScale = {};
                
                // misc
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
                            
                            var nest = d3.nest()
                                .key(function(d) { return d.alg_type; })
                                .entries(data);
                            
                            ////////////////////
                            ////// scales //////
                            ////////////////////
                            
                            // get dimensions excluding string-based columns
                            var dimensions = d3.keys(data[0]).filter(function(d) { return d != "name"; });
                            
                            // x scale
                            xScale.domain(dimensions);
                            
                            // y scale for each dimension
                            dimensions.forEach(function(d) {
                                
                                // add each scale to the y object
                                yScale[d] = d3.scale.linear()
                                    .domain(d3.extent(data, function(p) { return +p[d]; }))
                                    .range([activeHeight, padding.top]);
                                
                            });
                            
                            // color scale
                            cScale.domain(d3.map(nest, function(d) { return d.key; }));

                            ///////////////////
                            ////// lines //////
                            ///////////////////
                            
                            // background
                            background = canvas
                                .append("g")
                                .attr({
                                    class: "background"
                                })
                                .selectAll("path")
                                .data(data)
                                .enter()
                                .append("path")
                                .attr({
                                    d: path
                                });
                            
                            // focus
                            foreground = canvas
                                .append("g")
                                .attr({
                                    class: "foreground"
                                })
                                .selectAll("path")
                                .data(data)
                                .enter()
                                .append("path")
                                .attr({
                                    d: path
                                })
                                .style("stroke", function(d, i) { console.log(d); return cScale(d.alg_type); });

                            //////////////////
                            ////// axes //////
                            //////////////////
                            
                            // y axes group
                            var g = canvas
                                .selectAll(".dimension")
                                .data(dimensions)
                                .enter()
                                .append("g")
                                .attr({
                                    class: "dimension",
                                    transform: function(d) { return "translate(" + xScale(d) + ")"; }
                                });

                            // line
                            g.append("g")
                                .attr({
                                    class: "axis"
                                })
                                .each(function(d) {
                                    
                                    // update axis
                                    d3.select(this)
                                        .call(axis.scale(yScale[d]));
                                
                                })
                                .append("text")
                                .style("text-anchor", "end")
                                .attr({
                                    y: (padding.top / 2)
                                })
                                .text(function(d) { return d; });

                            // brush
                            g.append("g")
                                .attr({
                                    class: "brush"
                                })
                                .each(function(d) {
                                
                                    // update axis
                                    d3.select(this)
                                        .call(yScale[d].brush = d3.svg.brush()
                                              .y(yScale[d])
                                              .on("brush", brush));
                                
                                })
                                .selectAll("rect")
                                .attr({
                                    x: -8,
                                    width: 16
                                });

                            // return the path for a given data point
                            function path(d) {
                                return line(dimensions.map(function(p) {return [xScale(p), yScale[p](d[p])]; }));
                            };

                            // toggle lines
                            function brush() {
                            
                                var actives = dimensions.filter(function(p) { return !yScale[p].brush.empty(); });
                                var extents = actives.map(function(p) { return yScale[p].brush.extent(); });
                                
                                // update forground lines
                                foreground
                                    .attr({
                                        class: "active"
                                    })
                                    .style({
                                        display: function(d) {
                                            return actives.every(function(p, i) {
                                                return extents[i][0] <= d[p] && d[p] <= extents[i][1];
                                            }) ? null : "none";
                                        }
                                    });
                                
                            };

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);