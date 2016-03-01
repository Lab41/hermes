angular.module("parallel-coordinates-directive", [])

.directive("parallelCoordinates", ["d3Service", "$stateParams", "$state", function(d3Service, $stateParams, $state){
    return {
        restrict: "E",
        scope: {
            vizData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            colorRange: "=",
            transitionTime: "="
        },
        controller: function($scope) {
            
            // initial values
            $scope.options = { groupby: $stateParams.groupby };
			
			// change state via url
			$scope.changeState = function(groupby) {
				
				$state.go("app.viz", {
					groupby: groupby
				}, {
					reload: false,
					notify: false
				});
				
			};
            
        },
        templateUrl: "templates/directives/parallel-coordinates-plot.html",
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
                var transitionTime = parseInt(attrs.transitionTime) || 500;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
				
				///////////////////////////////////
                ////// basic layout settings //////
                ///////////////////////////////////
				
				var padding = { bottom: 10, left: 10, right: 10, top: 10 };
				var activeWidth = width - (padding.left + padding.right);
				var activeHeight = height - (padding.bottom + padding.top);
				
				///////////////////////////////////////////////////////
                ////// main svg constructs not dependent on data //////
                ///////////////////////////////////////////////////////
                                
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
				var xScale = d3.scale.ordinal()
                    .rangePoints([padding.left, activeWidth]);
                
                var cScale = d3.scale.ordinal()
                    .range(colorRange);
                
                var yScale = {};
                
                // misc
                var line = d3.svg.line();
				var axis = d3.svg.axis()
					.orient("left")
					.tickSize(0,0)
					.tickPadding(2);
                
                // de focus lines for context
				var background = canvas
                    .append("g")
                    .attr({
                        class: "background"
                    });
                
                // focus
                var foreground = canvas
                    .append("g")
                    .attr({
                        class: "foreground"
                    });
				
				///////////////////////////////////////////////
                ////// dynamic d3 runs every data update //////
                ///////////////////////////////////////////////
												
                // check for new data
                scope.$watchGroup(["vizData", "options.groupby"], function(newData, oldData) {
                    
                    // async check
                    if (newData[0] !== undefined && newData[1] !== undefined) {
                    
                        // check new vs old
                        var isMatching = angular.equals(newData, oldData);

                        // if false
                        //if (!isMatching) {

                            // update the viz
                            draw(newData);

                        //};
                        
                        function draw(data) {
                            
                            ///////////////////////////////////////////////////////////////////////
                            ////// assign variables (cleaner to read vs straight from scope) //////
                            ///////////////////////////////////////////////////////////////////////
                            
                            var rawValues = data[0].raw;
                            var groupby = data[1];
                            var labels = data[0].labels;
                            var axesOptions = data[0].options.axes;
							var groupOptions = data[0].options.groups;
                            var data = data[0].viz;
							var filtered = data.filter(function(d) { return d; }); // filter data based on user selection
                            
                            ///////////////////////////////////////
                            ////// assign to variables scope //////
                            ///////////////////////////////////////
                            
                            scope.labels = labels;
                            scope.axesOptions = axesOptions; // all drop down options
							scope.groupOptions = groupOptions; // group by options
                            scope.options.groupby = groupby; // group by value
							scope.filtered = filtered;
                            
                            var nest = d3.nest()
                                .key(function(d) { return d[groupby]; })
                                .entries(rawValues);
                            
                            ////////////////////
                            ////// scales //////
                            ////////////////////
                            
                            // get dimensions excluding string-based columns
                            var dimensions = d3.keys(data[0]);
                            
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
                            
                            // set selection
                            var bLine = background
                                .selectAll(".line")
                                .data(data);
                            
                            // update selection
                            bLine
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    d: path,
                                    class: "line"
                                });
                            
                            // enter selection
                            bLine
                                .enter()
                                .append("path")
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    d: path,
                                    class: "line"
                                });
                            
                            // exit selection
                            bLine
                                .exit()
                                .transition()
                                .duration(transitionTime)
                                .remove();
                            
                            // set selection
                            var fLine = foreground
                                .selectAll(".line")
                                .data(rawValues);
                            
                            // update selection
                            fLine
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    d: path,
                                    class: "line"
                                })
                                .style("stroke", function(d, i) { return cScale(d[groupby]); });
                            
                            // enter selection
                            fLine
                                .enter()
                                .append("path")
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    d: path,
                                    class: "line"
                                })
                                .style("stroke", function(d, i) { return cScale(d[groupby]); });
                            
                            // exit selection
                            fLine
                                .exit()
                                .transition()
                                .duration(transitionTime)
                                .remove();

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
                                .text(function(d) { return labels[d]; })
                                .style({
                                    "font-size": "0.2em"
                                });

                            // brush
                            g.append("g")
                                .attr({
                                    class: "brush"
                                })
								.on("click", function(d) { console.log(d); })
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
							
							// legend
                            
                            // set selction
                            var legend = d3.select(element.find("div")[0])
                                .selectAll(".item")
                                .data(nest);
                            
                            // update selection
                            legend
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    class: "item"
                                })
								.text(function(d) { return labels[d.key]; })
								.style({
									color: function(d) { return cScale([d.key]); }
								});
                            
                            // enter selection
                            legend
                                .enter()
                                .append("p")
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    class: "item"
                                })
								.text(function(d) { return labels[d.key]; })
								.style({
									color: function(d) { return cScale([d.key]); }
								});
                            
                            // exit selection
                            legend
                                .exit()
                                .transition()
                                .duration(transitionTime)
                                .remove();

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
                                    .selectAll("path")
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