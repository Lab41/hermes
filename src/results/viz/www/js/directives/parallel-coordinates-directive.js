angular.module("parallel-coordinates-directive", [])

.directive("parallelCoordinates", ["d3Service", "$stateParams", "$state", "dataService", function(d3Service, $stateParams, $state, dataService){
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
            $scope.options = { groupby: $stateParams.groupby, dimensions: {} };
			
			// change state via url
			$scope.changeState = function(groupby) {
				
				$state.go("app.viz", {
					groupby: groupby,
                    dimensions: $stateParams.dimensions
				}, {
					reload: false,
					notify: false
				});
				
			};
            
            $scope.changeAxis = function(axis) {
                
                var dimensions = $stateParams.dimensions.split(",");
                
                // check if item already in params
                var idx = dimensions.indexOf(axis);
                
                // set up new params for dimensions
                var newDimensions = idx == -1 ? $stateParams.dimensions + "," + axis : getDimensions(idx, dimensions);
                
                 // change state
                $state.go("app.viz", {
                    groupby: $stateParams.groupby,
                    dimensions: newDimensions
                }, {
                    reload: false,
                    notify: true
                });
				
                // data call before state change
                getStatic("parallel", "combined_results", { key: "structure", value: "parallel" }, newDimensions); // get for parallel coordinates plot
                                    
                function getDimensions(idx, dimensions) {
                    
                    var newArray = [];
                    
                    // check each item 
                    angular.forEach(dimensions, function(value, key) {
                        
                        // check index
                        if (key !== idx) {
                            
                            // add it to new array
                            this.push(value);
                            
                        };
                        
                    }, newArray);
                    
                    return newArray.toString();
                    
                };
                
			};
            
            // viz data
            function getStatic(format, name, query, params) {
                dataService.getStatic(name, query, params).then(function(data) {

                    // assign to scope
                    $scope.$parent[format + "Data"] = data;
                    
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
                // TODO fix watch so it's not hardcoded for values
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
                            var dims = $stateParams.dimensions.split(",");
                            var data = data[0].viz;
							var filtered = data.filter(function(d) { return d; }); // filter data based on user selection
                            
                            // add default dimensions
                            angular.forEach(dims, function(value, key) {
                                
                                scope.options.dimensions[value] = true;
                                
                            });
                            
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
                            
                            // get dimensions excluding angular specific hash key
                            var dimensions = d3.keys(data[0]).filter(function(d) { return d != "$$hashKey"; });
                            
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
                            
                            // bind events
                            fLine
                                .on({
                                    mouseover: function(d) {

                                        var tip = d3.select(element.find("div")[2]);

                                        tip.transition()		
                                            .duration(200)		
                                            .style("opacity", .9);		
                                        tip.html("id: " + d["Column1"])	
                                            .style("left", (d3.event.pageX) + "px")		
                                            .style("top", (d3.event.pageY - 28) + "px");
                                        
                                    },
									mouseout: function(d) {
                                        
                                        var tip = d3.select(element.find("div")[2]);
                                        
                                        tip.transition()
                                            .duration(200)
                                            .style("opacity", 0);
                                        
                                    }
                                })
                            
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
                            
                            // set selection
                            var yGroup = canvas
                                .selectAll(".dimension")
                                .data(dimensions);
                            
                            // update selection
                            yGroup
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    class: "dimension",
                                    transform: function(d) { return "translate(" + xScale(d) + ")"; }
                                })
                                .each(function(d) {
                                
                                    var dGroup = d3.select(this);
                                    var dItem = d;
                                
                                    // AXIS
                                
                                    // set selection
                                    var aGroup = dGroup
                                        .selectAll(".axis")
                                        .data([dItem]);
                                
                                    // update selection
                                    aGroup
                                        .attr({
                                            class: "axis"
                                        })
                                        .each(function(d) {
                                        
                                            // set selection
                                            var container = d3.select(this);
                                        
                                            // axis
                                        
                                            // update axis
                                            container
                                                .transition()
                                                .duration(transitionTime)
                                                .call(axis.scale(yScale[d]));
                                        
                                            // text
                                        
                                            // set selection
                                            var aText = container
                                                .selectAll(".label")
                                                .data([d]);
                                        
                                            // update text
                                            aText
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // enter text
                                            aText
                                                .enter()
                                                .append("text")
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // exit text
                                            aText
                                                .exit()
                                                .transition()
                                                .duration(transitionTime)
                                                .remove();
                                        
                                        });
                                
                                    // enter selection
                                    aGroup
                                        .enter()
                                        .append("g")
                                        .attr({
                                            class: "axis"
                                        })
                                        .each(function(d) {
                                        
                                            // set selection
                                            var container = d3.select(this);
                                        
                                            // axis
                                        
                                            // update axis
                                            container
                                                .transition()
                                                .duration(transitionTime)
                                                .call(axis.scale(yScale[d]));
                                        
                                            // text
                                        
                                            // set selection
                                            var aText = container
                                                .selectAll(".label")
                                                .data([d]);
                                        
                                            // update text
                                            aText
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // enter text
                                            aText
                                                .enter()
                                                .append("text")
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // exit text
                                            aText
                                                .exit()
                                                .transition()
                                                .duration(transitionTime)
                                                .remove();
                                        
                                        });
                                
                                    // exit selection
                                    aGroup
                                        .exit()
                                        .transition()
                                        .duration(transitionTime)
                                        .remove();
                                
                                    // BRUSH
                                
                                    // set selection
                                    var bGroup = dGroup
                                        .selectAll(".brush")
                                        .data([dItem]);
                                
                                    // update selection
                                    bGroup
                                        .attr({
                                            class: "brush"
                                        })
                                        .each(function(d) { d3.select(this).call(yScale[d].brush = d3.svg.brush().y(yScale[d]).on("brush", brush)); });
                                
                                    // enter selection
                                    bGroup
                                        .enter()
                                        .append("g")
                                        .attr({
                                            class: "brush"
                                        })
                                        .each(function(d) { d3.select(this).call(yScale[d].brush = d3.svg.brush().y(yScale[d]).on("brush", brush)); });
                                
                                    // exit selection
                                    bGroup
                                        .exit()
                                        .remove();

                                });
                            
                            // enter selection
                            yGroup
                                .enter()
                                .append("g")
                                .transition()
                                .duration(transitionTime)
                                .attr({
                                    class: "dimension",
                                    transform: function(d) { return "translate(" + xScale(d) + ")"; }
                                })
                                .each(function(d) {
                                
                                    var dGroup = d3.select(this);
                                    var dItem = d;
                                
                                    // AXIS
                                
                                    // set selection
                                    var aGroup = dGroup
                                        .selectAll(".axis")
                                        .data([dItem]);
                                
                                    // update selection
                                    aGroup
                                        .attr({
                                            class: "axis"
                                        })
                                        .each(function(d) {
                                        
                                            // set selection
                                            var container = d3.select(this);
                                        
                                            // axis
                                        
                                            // update axis
                                            container
                                                .transition()
                                                .duration(transitionTime)
                                                .call(axis.scale(yScale[d]));
                                        
                                            // text
                                        
                                            // set selection
                                            var aText = container
                                                .selectAll(".label")
                                                .data([d]);
                                        
                                            // update text
                                            aText
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // enter text
                                            aText
                                                .enter()
                                                .append("text")
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // exit text
                                            aText
                                                .exit()
                                                .transition()
                                                .duration(transitionTime)
                                                .remove();
                                        
                                        });
                                
                                    // enter selection
                                    aGroup
                                        .enter()
                                        .append("g")
                                        .attr({
                                            class: "axis"
                                        })
                                        .each(function(d) {
                                        
                                            // set selection
                                            var container = d3.select(this);
                                        
                                            // axis
                                        
                                            // update axis
                                            container
                                                .transition()
                                                .duration(transitionTime)
                                                .call(axis.scale(yScale[d]));
                                        
                                            // text
                                        
                                            // set selection
                                            var aText = container
                                                .selectAll(".label")
                                                .data([d]);
                                        
                                            // update text
                                            aText
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // enter text
                                            aText
                                                .enter()
                                                .append("text")
                                                .transition()
                                                .duration(transitionTime)
                                                .attr({
                                                    class: "label",
                                                    y: (padding.top / 2)
                                                })
                                                .text(function(d) { return labels[d]; });
                                        
                                            // exit text
                                            aText
                                                .exit()
                                                .transition()
                                                .duration(transitionTime)
                                                .remove();
                                        
                                        });
                                
                                    // exit selection
                                    aGroup
                                        .exit()
                                        .transition()
                                        .duration(transitionTime)
                                        .remove();
                                
                                    // BRUSH
                                
                                    // set selection
                                    var bGroup = dGroup
                                        .selectAll(".brush")
                                        .data([dItem]);
                                
                                    // update selection
                                    bGroup
                                        .attr({
                                            class: "brush"
                                        })
                                        .each(function(d) { d3.select(this).call(yScale[d].brush = d3.svg.brush().y(yScale[d]).on("brush", brush)); })
                                        .selectAll("rect")
                                        .attr({
                                            x: -8,
                                            width: 16
                                        });
                                
                                    // enter selection
                                    bGroup
                                        .enter()
                                        .append("g")
                                        .attr({
                                            class: "brush"
                                        })
                                        .each(function(d) { d3.select(this)
                                            .call(yScale[d].brush = d3.svg.brush().y(yScale[d]).on("brush", brush)); })
                                        .selectAll("rect")
                                        .attr({
                                            x: -8,
                                            width: 16
                                        });
                                
                                    // exit selection
                                    bGroup
                                        .exit()
                                        .remove();

                                });
                            
                            // exit selection
                            yGroup
                                .exit()
                                .transition()
                                .duration(transitionTime)
                                .remove();
                            
							
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
                                var selected = [];
                                
                                // update forground lines
                                foreground
                                    .selectAll("path")
                                    .attr({
                                        class: "active"
                                    })
                                    .style({
                                        display: function(d) {
                                            return actives.every(function(p, i) {
                                                
                                                // capture selected items in brush
                                                if (extents[i][0] <= d[p] && d[p] <= extents[i][1]) {
                                                    selected.push(d);
                                                }
                                                
                                                return extents[i][0] <= d[p] && d[p] <= extents[i][1];
                                            }) ? null : "none";
                                        }
                                    });
                                
                                // add items to scope
                                scope.selected = selected;
                                
                                scope.$apply();

                            };

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);