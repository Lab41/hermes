angular.module("scatter-plot-directive", [])

.directive("scatterPlot", ["d3Service", function(d3Service){
    return {
        restrict: "E",
        scope: {
            vizData: "=",
            labelData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            colorRange: "="
        },
        controller: function($scope) {
            
            // initial values
            $scope.options = { xSelect: 0, ySelect: 1, algos: [], algoSelect: {} };
            
            // number available for axis
            $scope.axis = 13;
            
        },
        templateUrl: "templates/scatter-plot.html",
        link: function(scope, element, attrs){
            
            //get d3 promise
            d3Service.d3().then(function(d3) {
                
                // set sizes from attributes in html element
                // if not attributes present - use default
                var width = parseInt(attrs.canvasWidth) || 400;
                var height = parseInt(attrs.canvasHeight) || 200;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
				
				var radius = 8;
				var diameter = radius * 2;
				var padding = { bottom: 20, left: radius, right: 10, top: 5 };
				var activeWidth = width - (padding.left + padding.right);
				var activeHeight = height - (padding.bottom + padding.top);
                                
                // create svg canvas
                var canvas = d3.select(element.find("svg")[0])
                    .attr({
                        viewBox: "0 0 " + width + " " + height
                    })
					.append("g")
					.attr({
						transform: "translate(" + padding.left + "," + padding.top + ")"
					});

                // scales
				var xScale = d3.scale.linear()
					.range([0, activeWidth]);
				
				var yScale = d3.scale.linear()
					.range([activeHeight, 0]);
                
                var cScale = d3.scale.ordinal()
                    .range(colorRange);
				
				// axis
				var xAxis = d3.svg.axis()
					.scale(xScale)
					.tickSize(0,0)
					.orient("bottom");
				
				var yAxis = d3.svg.axis()
					.scale(yScale)
					.tickSize(-(activeWidth), 2)
					.tickPadding(0)
					.orient("left");
												
                // check for new data
                scope.$watchGroup(["vizData", "labelData", "options.xSelect", "options.ySelect"], function(newData, oldData) {
                    
                    // async check
                    if (newData[0] !== undefined && newData[1] !== undefined) {
                    
                        // check new vs old
                        var isMatching = angular.equals(newData, oldData);

                        // if false
                        if (!isMatching) {

                            // update the viz
                            draw(newData);

                        } else {

                            // initial render
                            draw(newData);
                            
                        };
                        
                        function draw(data) {
                        
                            var labels = data[1];
                            var options = data[2];
                            var data = data[0];
                            var optionsData = [];
							var tableHeader = {};
							var algosSelected = {};
							var selected = "";
                            
							// algorithm options
                            angular.forEach(labels, function(value, key) {
                                
                                // check index
                                if (key < scope.axis) {
                                    
                                    this.push(value);
                                    
                                };
                                
                            }, optionsData);
                            
                            // nest by types
                            var nest = d3.nest()
                                .key(function(d) { return d.alg_type; })
                                .entries(data);
                            
                            // set up checkbox scopes for binding
                            angular.forEach(nest, function(value, key) {
                                
                                // add to options scope
                                // by default make all checked
								algosSelected[value.key] = true; 
                                
                            });
                            
                            // table headers
                            angular.forEach(labels, function(value, key) {
                                
                                tableHeader[value.raw] = value.label;
                                
                            });
							
							// selected chart labels
							angular.forEach(algosSelected, function(value, key) {
								
								// check for selection
								if (value) {
									
									selected += key + "|";
									
								};
								
							});
							
							// filter data based on user selection
							var filtered = data.filter(function(d, i) {
								
								// check for selected algorithms
								return selected.match(d.alg_type);
								
							});
                            
							// assign to scope
							scope.options.algos = nest;
							scope.options.algoSelect = algosSelected;
							scope.optionsData = optionsData;
							scope.filtered = filtered;
                            scope.runCount = filtered.length;
                            scope.tableHeader = tableHeader;
							
							// set scale domains with *nice* round numbers
							xScale.domain(d3.extent(data, function(d) { return parseFloat(d[labels[scope.options.xSelect].raw]); })).nice();
							yScale.domain(d3.extent(data, function(d) { return parseFloat(d[labels[scope.options.ySelect].raw]); })).nice();
                            cScale.domain(d3.map(nest, function(d) { return d.key; }));
							
							// y axis
							canvas
                                .append("g")
								.attr({
									class: "y-axis"
								})
								.call(yAxis);
							
							// customize text
							canvas.select(".y-axis")
								.selectAll("text")
								.attr({
									dy: "1em" // TODO dynamically pull this value from computed CSS font size
								})
								.style({
									"text-anchor": "start"
								});
							
							// x axis
							canvas.append("g")
								.attr({
									class: "x-axis",
									transform: "translate(0," + activeHeight + ")"
								})
								.call(xAxis);
                            
                            // customize text
							canvas.select(".x-axis")
								.selectAll("text")
								.attr({
									dy: "1em" // TODO dynamically pull this value from computed CSS font size
								});
                            
							// scatter
                            
                            // set selection
							var circle = canvas
                                .selectAll(".dot")
								.data(filtered);
                            
                            // update selection
                            circle
                                .transition()
                                .duration(500)
								.attr({
									class: "dot",
									r: radius,
									cx: function(d) { return xScale(d[labels[scope.options.xSelect].raw]); },
									cy: function(d) { return yScale(d[labels[scope.options.ySelect].raw]); }
								})
								.style("fill", function(d, i) { return cScale(d.alg_type); });
                            
                            // enter selection
                            circle
                                .enter()
								.append("circle")
                                .transition()
                                .duration(500)
                                .attr({
									class: "dot",
									r: radius,
									cx: function(d) { return xScale(d[labels[scope.options.xSelect].raw]); },
									cy: function(d) { return yScale(d[labels[scope.options.ySelect].raw]); }
								})
								.style("fill", function(d, i) { return cScale(d.alg_type); });
                            
                            // selection events
                            circle
                                .on({
                                    mouseover: function(d) {

                                        var tip = d3.select(element.find("div")[2]);

                                        tip.transition()		
                                            .duration(200)		
                                            .style("opacity", .9);		
                                        tip.html(d.content_vector)	
                                            .style("left", (d3.event.pageX) + "px")		
                                            .style("top", (d3.event.pageY - 28) + "px");
                                        
                                    },
                                    click: function(d) {

                                        // clear all active styles
                                        d3.selectAll(".dot")
                                            .transition()
                                            .duration(200)
                                            .style({
                                                opacity: 0.1
                                            });

                                        // add active style
                                        d3.select(this)
                                            .transition()
                                            .delay(200)
                                            .duration(200)
                                            .style({
                                                opacity: 1.0,
                                                stroke: "white",
                                                "stroke-width": 2
                                            });

                                        // assign sope so row highlights
                                        scope.showRow = d.Column1;

                                        // trigger $digest
                                        scope.$apply();

                                    }
                                });
                            
                            // exit selection
                            circle
                                .exit()
                                .transition()
                                .duration(5000)
                                .remove();

                            // legend
                            
                            // set selction
                            var legend = d3.select(element.find("div")[0])
                                .selectAll(".item")
                                .data(cScale.domain());
                            
                            // update selection
                            legend
                                .transition()
                                .duration(500)
                                .attr({
                                    class: "item"
                                });
                            
                            // enter selection
                            legend
                                .enter()
                                .append("p")
                                .transition()
                                .duration(500)
                                .attr({
                                    class: "item"
                                })
                            
                                // each legend item
                                .each(function(d) {

                                    var item = d3.select(this);

                                    // label shape
                                    item
                                        .append("span")
                                        .html("&#x25A0;")
                                        .style({
                                            color: cScale
                                        });

                                    // label text
                                    item
                                        .append("span")
                                        .text(function(d) { return tableHeader[d]; });

                                });
                            
                            // exit selection
                            legend
                                .exit()
                                .transition()
                                .duration(500)
                                .remove();
                            
                            // Then transition the y-axis.
                            /*yScale.domain(d3.extent(data, function(d) { return parseFloat(d[labels[scope.options.ySelect].raw]); })).nice();
                            
                            canvas
                                .transition()
                                .duration(500)
                                .selectAll(".y-axis")
                                .call(yAxis);*/

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);