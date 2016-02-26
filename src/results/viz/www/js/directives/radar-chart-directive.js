angular.module("radar-chart-directive", [])

.directive("radarChart", ["d3Service", function(d3Service){
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
            
            // number available for axis
            $scope.axis = 3;
            
        },
        template: "<h1>{{ tableHeader[vizData.name] }}</h1>",
        link: function(scope, element, attrs){
            
            //get d3 promise
            d3Service.d3().then(function(d3) {
                
                // set sizes from attributes in html element
                // if not attributes present - use default
                var width = parseInt(attrs.canvasWidth) || 400;
                var height = parseInt(attrs.canvasHeight) || width;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
				
				var padding = { bottom: 20, left: 20, right: 20, top: 20 };
                var diameter = width - padding.bottom - padding.left - padding.right - padding.top;
				var radius = diameter / 2;
				var activeWidth = width - (padding.left + padding.right);
				var activeHeight = height - (padding.bottom + padding.top);
                                
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
                
                // scales
                
                // radius scale (like the y scale)
				var rScale = d3.scale.linear()
                    .domain([0, 0.5])
					.range([0, radius]);
												
                // check for new data
                scope.$watchGroup(["vizData", "labelData"], function(newData, oldData) {
                    
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
                            console.log(data[0]);
                            var labels = data[1];
                            var data = data[0];
                            // convert data to format we need
                            // do server-side eventually

                            // get number of radials
                            var radialCount = scope.axis;
                            
                            // table headers
                            var tableHeader = {};
                            
                            angular.forEach(labels, function(value, key) {
                                
                                tableHeader[value.raw] = value.label;
                                
                            });
                            
                            scope.tableHeader = tableHeader;
                            
                            // set scale domains with *nice* round numbers
							//rScale.domain(d3.extent(data, function(d) { console.log(d);return parseFloat(d[labels["avg_lowest_rank"]]); })).nice();
                            
                            // radial axis
                            var rAxis = canvas
                                .append("g")
                                .attr({
                                    class: "r-axis"
                                })
                                .selectAll("g")
                                .data(rScale.ticks(5).splice(1))
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
                                .text(function(d) { return d; });*/
                            
                            var categories = d3.range(0, 360, (360 / radialCount));
                            
                            // set up data object for line values
                            var lineValues = { categories: categories, values: [] };
                            
                            angular.forEach(data.values, function(value, key) {
                                console.log(value);
                            })
                            
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
                                    transform: function(d) { return "rotate(" + -d + ")"; }
                                });
                            
                            // line
                            aAxis
                                .append("line")
                                .attr({
                                    x2: radius
                                });
                            
                            // line lable
                            aAxis
                                .append("text")
                                .attr({
                                    x: 0,
                                    y: radius
                                })
                                .text(function(d, i) { return labels[i].label; });
                            
                            // add starting point to end of line to close the path
                            categories.push(categories[0]);

                            // value line
                            var line = d3.svg.line.radial()
                                .radius(function(d) { return /*rScale(d[1]);*/rScale(0.2); }) // radial scale (y)
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