angular.module("heatmap-grid-directive", [])

.directive("heatmapGrid", ["d3Service", "moment", function(d3Service, moment){
    return {
        restrict: "E",
        scope: {
            vizData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            steps: "=",
            gutter: "=",
            padding: "=",
            yAxisWidth: "=",
            xAxisHeight: "=",
            colorRange: "="
        },
        link: function(scope, element, attrs){
            
            //get d3 promise
            d3Service.d3().then(function(d3) {
                
                // set sizes from attributes in html element
                // if not attributes present - use default
                var width = parseInt(attrs.canvasWidth) || 400;
                var height = parseInt(attrs.canvasHeight) || 200;
                var steps = parseInt(attrs.steps) || 5;
				var gutter = parseInt(attrs.gutter) || 10;
				var padding = parseInt(attrs.padding) || 20;
                var yLabelColumn = (parseInt(attrs.yAxisWidth) || 20) / 100;
                var xLabelRow = (parseInt(attrs.xAxisHeight) || 10) / 100;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "darkgrey", "grey", "white"];
                
                // convert values to usable space calculations
                var activeXspace = width - (width * yLabelColumn);
                var activeYspace = height - (height * xLabelRow);
                                
                // create sv canvas
                var canvas = d3.select(element[0])
                    .append("svg")
                    .attr({
                        viewBox: "0 0 " + width + " " + height
                    });
                
                // add group for grid
                var gridWrap = canvas
                    .append("g")
					.attr({
						id: "heat-grid",
						transform: "translate(" + (padding + (yLabelColumn * width)) + ", " + (padding + (xLabelRow * height)) + ")"
					});
                
                // add group for y labels
                var yLabelWrap = canvas
                    .append("g")
                    .attr({
						id: "heat-y-header",
                        transform: "translate(" + padding + "," + (padding + (xLabelRow * height)) + ")"
                    });
                
                // add group for x labels
                var xLabelWrap = canvas
                    .append("g")
                    .attr({
						id: "heat-x-header",
                        transform: "translate(" + (padding + (yLabelColumn * width)) + "," + padding + ")"
                    });
				
                // check for new data
                scope.$watch("vizData", function(newData, oldData) {
                    
                    // async check
                    if (newData !== undefined) {
                    
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

                            // rows/columns
                            var columnCount = data[0].dates.length;
                            var rowCount = data.length;

                            // active space minus gutters and padding
                            var contentWidth = (activeXspace - (padding * 2) - (gutter * (columnCount - 1)));
                            var contentHeight = (activeYspace - (padding * 2) - (gutter * (rowCount - 1)));

                            // dimension of a single grid item
                            var tileWidth = contentWidth / columnCount; 
                            var tileHeight = contentHeight / rowCount;

                            // create color scale
                            var color = d3.scale.quantile()
                                .domain([0, steps - 1, /*d3.max(newData.values, function (d) { return d.value; })*/ 100])
                                .range(colorRange);
					
                            /****************/
                            /***** GRID *****/
                            /****************/

                            // set selection
                            var grid = d3.select("#heat-grid")
                                .selectAll(".row")
                                .data(data);

                            // enter selection
                            grid
                                .enter()
                                .append("g");

                            // update selection
                            grid
                                .each(function(row, i) {

                                // set row
                                var rowIdx = i;

                                // set selection
                                var columnGroup = d3.select(this)
                                    .selectAll(".tile")
                                    .data(row.dates);

                                // enter selection
                                columnGroup
                                    .enter()
                                    .append("g");

                                // update selection 
                                columnGroup
                                    .each(function(date, i) {

                                        // set date
                                        var dayIdx = i;

                                        // set wrap that holds tile and label
                                        var dayGroup = d3.select(this);

                                        // set selection
                                        var shape = dayGroup
                                            .selectAll("rect")
                                            .data([date]);

                                        // enter selection
                                        shape
                                            .enter()
                                            .append("rect");

                                        // update selection
                                        shape
                                            .transition()
                                            .duration(1000)
                                            .attr({
                                                x: dayIdx * (tileWidth + gutter),
                                                y: rowIdx  * (tileHeight + gutter),
                                                width: tileWidth,
                                                height: tileHeight
                                            })
                                            .style({
                                                fill: function(d) { return color(d.value); }
                                            });

                                        // set selection
                                        var label = dayGroup
                                            .selectAll("text")
                                            .data([date]);

                                        // enter selection
                                        label
                                            .enter()
                                            .append("text");

                                        // update selection
                                        label
                                            .style({
                                                opacity: 0
                                            })
                                            .transition()
                                            .duration(1000)
                                            .delay(500)
                                            .attr({
                                                class: "label",
                                                x: (dayIdx * (tileWidth + gutter)) + (tileWidth / 2),
                                                y: (rowIdx  * (tileHeight + gutter)) + (tileHeight / 2),
                                            })
                                            .style({
                                                opacity: 1
                                            })
                                            .text(function(d) { return d.value; });

                                    })
                                    .attr({
                                        class: "tile"
                                    });

                                // exit selection
                                columnGroup
                                    .exit()
                                    .remove();

                                })
                                .attr({
                                    class: "row"
                                });

                            // exit selection
                            grid
                                .exit()
                                .remove();

                             /******************/
                            /***** Y-AXIS *****/
                            /******************/

                            // set selection
                            var column = d3.select("#heat-y-header")
                                .selectAll("text")
                                .data(data);

                            // enter selection
                            column
                                .enter()
                                .append("text");

                            // update selection
                            column
                                .style({
                                    opacity: 0
                                })
                                .transition()
                                .duration(1000)
                                .attr({
                                    class: "y-axis",
                                    x: 0,
                                    y: function(d, i) { return (i  * (tileHeight + gutter)) + (tileHeight / 2) },
                                })
                                .style({
                                    opacity: 1
                                })
                                .text(function(d) { return d.name; });

                            // exit selection
                            column
                                .exit()
                                .remove();

                            /******************/
                            /***** X-AXIS *****/
                            /******************/

                            // set selection
                            var header = d3.select("#heat-x-header")
                                .selectAll("text")
                                .data(data[0].dates);

                            // enter selection
                            header
                                .enter()
                                .append("text");

                            // update selection
                            header
                                .style({
                                    opacity: 0
                                })
                                .attr({
                                    class: "x-axis",
                                    x: function(d, i) { return (i * (tileWidth + gutter)) + (tileWidth / 2) },
                                    y: (xLabelRow * height / 2),
                                })
                                .style({
                                    opacity: 1
                                })
                                .text(function(d, i) { 

                                    // get month digit
                                    var month = data[0].dates[i - 1];

                                    // need to just filter data out
                                    // this ensures the month isn't repeated over and over
                                    if (month == undefined) {

                                        // first instance of a month digit
                                        // and first date in grid
                                        return moment(d.date).fromNow();

                                    } else if (month.date.split("-")[1] != d.date.split("-")[1]) {

                                        return moment(d.date).fromNow(); 

                                    };

                                });

                            // exit selection
                            header
                                .exit()
                                .remove();

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);