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
                var canvas = d3.select(element[0])
                    /*.attr({
                        viewBox: "0 0 " + width + " " + height
                    })
					.append("g")
					.attr({
						transform: "translate(" + padding.left + "," + padding.top + ")"
					});*/
												
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
                            
                            var columns = Object.keys(data[0]);

                            var chart = d3.parsets()
      .dimensions(columns);

	var vis = canvas
        .attr("width", chart.width())
		.attr("height", chart.height());

	  vis.datum(data).call(chart);


                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);