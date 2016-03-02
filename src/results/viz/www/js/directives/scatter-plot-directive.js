angular.module("scatter-plot-directive", [])

.directive("scatterPlot", ["d3Service", "$stateParams", "$state", function(d3Service, $stateParams, $state){
    return {
        restrict: "E",
        scope: {
            vizData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            colorRange: "=",
            transitionTime: "=",
            scaleReadable: "="
        },
        controller: function($scope) {
            
            // initial values
            $scope.options = { xSelect: {}, ySelect: {}, groupby: $stateParams.groupby };
			
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
        templateUrl: "templates/directives/scatter-plot.html",
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
                var transitionTime = parseInt(attrs.transitionTime) || 200;
                var scaleReadable = ["poor", "ok", "neutral", "good", "excellent"];
                console.log(scaleReadable);
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
				
                ///////////////////////////////////
                ////// basic layout settings //////
                ///////////////////////////////////
                
				var radius = 8;
				var diameter = radius * 2;
				var padding = { bottom: 20, left: radius, right: 10, top: 5 };
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
                    .ticks(5)
                    //.tickValues(scaleReadable)
					.orient("bottom");
				
				var yAxis = d3.svg.axis()
					.scale(yScale)
					.tickSize(-(activeWidth), 2)
					.tickPadding(0)
					.orient("left");
                
                ///////////////////////////////////////////////
                ////// dynamic d3 runs every data update //////
                ///////////////////////////////////////////////
												
                // check for new data
                scope.$watchGroup(["vizData", "options.xSelect", "options.ySelect", "options.groupby"], function(newData, oldData) {
                    
                    // async check
                    if (newData[0] !== undefined && newData[3] !== undefined) {
                    
                        // check new vs old
                        var isMatching = angular.equals(newData, oldData);

                        // if false
                        //if (!isMatching) {

                            // update the viz
                            draw(newData);

                        //};
                        
                        function draw(data) {console.log(data);
                                             
                            ///////////////////////////////////////////////////////////////////////
                            ////// assign variables (cleaner to read vs straight from scope) //////
                            ///////////////////////////////////////////////////////////////////////
                        
                            var labels = data[0].labels;
                            var axesOptions = data[0].options.axes;
							var groupOptions = data[0].options.groups;
                            var groupby = data[3];
                                             
                            ///////////////////////////////////////
                            ////// assign to variables scope //////
                            ///////////////////////////////////////
                                             
							scope.labels = labels;
                            scope.axesOptions = axesOptions; // all drop down options
							scope.groupOptions = groupOptions; // group by options
                            scope.options.xSelect = Object.keys(data[1]).length == 0 ? axesOptions[11] : data[1]; // dropdown value for x-axis as chart redraws
                            scope.options.ySelect = Object.keys(data[2]).length == 0 ? axesOptions[4] : data[2]; // dropdown value for y-axis as chart redraws
							scope.options.groupby = groupby; // group by value
                                             
                            var xSelect = data[1].raw;
                            var ySelect = data[2].raw;
							var groupby = data[3];
                            var data = data[0].viz; // sloppy naming here but easier to understand in all the d3 below
							var filtered = data.filter(function(d) { return d; }); // filter data based on user selection
											 
							// nest by group by value
                            var nest = d3.nest()
                                .key(function(d) { return d[groupby]; })
                                .entries(data);
                                             
							// assign to scope
							scope.filtered = filtered;
                                             
                            //////////////////////////
                            ////// scales & axis//////
                            //////////////////////////
                        
							// set scale domains with *nice* round numbers
							xScale.domain(d3.extent(data, function(d) { return d[xSelect]; })).nice();
							yScale.domain(d3.extent(data, function(d) { return d[ySelect]; })).nice();
                            cScale.domain(d3.map(nest, function(d) { return d.key; }));
							
							// y axis
							canvas
                                .append("g")
								.attr({
									class: "y-axis"
								});
                                             
                            // transition y-axis
                            canvas
                                .select(".y-axis")
								.transition()
                                .duration(transitionTime)
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
							canvas
                                .append("g")
								.attr({
									class: "x-axis",
									transform: "translate(0," + activeHeight + ")"
								});
                              
                            // transition x-axis
                            canvas
                                .select(".x-axis")
								.transition()
                                .duration(transitionTime)
                                .call(xAxis);
                            
                            // customize text
							canvas.select(".x-axis")
								.selectAll("text")
								.attr({
									dy: "1em" // TODO dynamically pull this value from computed CSS font size
								})
                            
							// scatter
                            
                            // set selection
				            var circle = canvas
                                .selectAll(".dot")
								.data(filtered);
                            
                            // update selection
                            circle
                                .transition()
                                .duration(transitionTime)
								.attr({
									class: "dot",
									r: radius,
									cx: function(d) { return xSelect == undefined ? -(diameter) : xScale(d[xSelect]); },
									cy: function(d) { return ySelect == undefined ? -(diameter) : yScale(d[ySelect]); }
								})
								.style("fill", function(d, i) { return cScale(d[groupby]); });
                            
                            // enter selection
                            circle
                                .enter()
								.append("circle")
                                .transition()
                                .duration(transitionTime)
                                .attr({
									class: "dot",
									r: radius,
									cx: function(d) { return xSelect == undefined ? -(diameter) : xScale(d[xSelect]); },
									cy: function(d) { return ySelect == undefined ? -(diameter) : yScale(d[ySelect]); }
								})
								.style("fill", function(d, i) { return cScale(d[groupby]); });
                            
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
									mouseout: function(d) {
                                        
                                        var tip = d3.select(element.find("div")[2]);
                                        
                                        tip.transition()
                                            .duration(200)
                                            .style("opacity", 0);
                                        
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

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);