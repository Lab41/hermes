angular.module("radar-chart-directive", [])

.directive("radarChart", ["d3Service", function(d3Service){
    return {
        restrict: "E",
        scope: {
            vizData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            colorRange: "="
        },
        template: "<h1>{{ title }}</h1>",
        link: function(scope, element, attrs){
            
            //get d3 promise
            d3Service.d3().then(function(d3) {
				
				/////////////////////////////////////////
                ////// values from html attributes //////
                /////////////////////////////////////////
                
                // set sizes from attributes in html element
                // if not attributes present - use default
                var width = parseInt(attrs.canvasWidth) || 400;
                var height = parseInt(attrs.canvasHeight) || width;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
				
				///////////////////////////////////
                ////// basic layout settings //////
                ///////////////////////////////////
				
				var padding = { bottom: 20, left: 20, right: 20, top: 20 };
                var diameter = width - padding.bottom - padding.left - padding.right - padding.top;
				var radius = diameter / 2;
				var activeWidth = width - (padding.left + padding.right);
				var activeHeight = height - (padding.bottom + padding.top);
				var rPadding = 6; // padding of text around outermost circle
                var ringCount = 4; // number of ticks on radial scales
				
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
						transform: "translate(" + (width / 2) + "," + (width / 2) + ")"
					});
                                
                // radius scale (rings)
				var rScale = d3.scale.linear()
                    .domain([0, radius])
					.range([0, radius]);
                
                // attribute scale (paths)
                var aScale = {};
				
				///////////////////////////////////////////////
                ////// dynamic d3 runs every data update //////
                ///////////////////////////////////////////////
												
                // check for new data
                scope.$watchGroup(["vizData"], function(newData, oldData) {
                    
                    // async check
                    if (newData[0] !== undefined) {
                    
                        // check new vs old
                        var isMatching = angular.equals(newData, oldData);

                        // if false
                        //if (!isMatching) {

                            // update the viz
                            draw(newData);

                        //};
                        
                        function draw(data) {
							console.log(data);
							///////////////////////////////////////////////////////////////////////
                            ////// assign variables (cleaner to read vs straight from scope) //////
                            ///////////////////////////////////////////////////////////////////////
							
                            var data = data[0];
							var aKeys = Object.keys(data.values[0]); // attributes
							
							///////////////////////////////////////
                            ////// assign to variables scope //////
                            ///////////////////////////////////////
							scope.title = data.key;
                            
							///////////////////////////
                            ////// scales & axis //////
                            ///////////////////////////
                            
                            // y scale for each dimension
                            aKeys.forEach(function(d) {
                                
                                // add each scale to the y object
                                aScale[d] = d3.scale.linear()
                                    .domain(d3.extent(data.values, function(p) { return p[d]; }))
                                    .range([0, radius]);
                                
                            });
							
                            // radial axis
                            var rAxis = canvas
                                .append("g")
                                .attr({
                                    class: "r-axis"
                                })
                                .selectAll("g")
                                .data(rScale.ticks(ringCount))
                                .enter()
                                .append("g");
                            
                            // rings
                            rAxis
                                .append("circle")
                                .attr({
                                    r: rScale
                                });
                            
                            // ring label
                            /*rAxis
                                .append("text")
                                .attr({
                                    y: function(d) { return -rScale(d); }
                                })
                                .text(function(d) { return d; })
								.style({
									"font-size": "0.7em",
									fill: "red"
								});*/
                            
                            var categories = d3.range(0, 360, (360 / aKeys.length));
                            
                            // attribute axis
                            var aAxis = canvas
                                .append("g")
                                .attr({
                                    class: "a-axis"
                                })
                                .selectAll("g")
                                .data(categories)
                                .enter()
                                .append("g")
                                .attr({
                                    transform: function(d) { return "rotate(" + d + ")"; }
                                })
                            
                                .each(function(d, i) {
                                    
                                    var aGroup = d3.select(this);
                                    var ringAngle = d;
                                    var attrIdx = i;
                                    
                                    // line
                                    aGroup
                                        .append("line")
                                        .attr({
                                            x2: radius
                                        });
console.log(aScale[aKeys[attrIdx]].ticks(ringCount));
                                    aGroup
                                        .selectAll(".tick")
                                        .data(aScale[aKeys[attrIdx]].ticks(ringCount))
                                        .enter()
                                        .append("text")
                                        .attr({
                                            class: "tick",
                                            dx: function(d, i){ return ringAngle < 270 && ringAngle > 90 ? -aScale[aKeys[attrIdx]](d) : aScale[aKeys[attrIdx]](d); },
                                            dy: 0,
                                            transform: ringAngle < 270 && ringAngle > 90 ? "rotate(180)" : null
                                        })
                                        .text(function(d) { return d; })
                                        .style({
                                            "font-size": "0.65em",
                                            "text-anchor": "start"
                                        });

                                    // line lable
                                    aGroup
                                        .append("text")
                                        .attr({
                                            x: radius + rPadding,
                                            dy: 0,
                                            transform: ringAngle < 270 && ringAngle > 90 ? "rotate(180 " + (radius + rPadding) + ",0)" : null
                                        })
                                        .text(aKeys[attrIdx])
                                        .style({
                                            "text-anchor": ringAngle < 270 && ringAngle > 90 ? "end" : null,
                                            "font-size": "0.5em"
                                        });
                                    
                                });
							                            
                            // add starting point to end of line to close the path
                            categories.push(categories[0]);

							//////////////////////////
                            ////// radial paths //////
                            //////////////////////////
							
                            // value line
                            var line = d3.svg.line.radial()
                                .radius(function(d) { return /*rScale(d[1]);*/rScale(0.13); }) // radial scale (y)
                                .angle(function(d) { return -(d * (Math.PI / 180)) + Math.PI / 2; }); // attribute scale (x)
                            
                            canvas
                                .append("path")
                                .datum(categories)
                                .attr({
                                    class: "line",
                                    d: line
                                });

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);