angular.module("packed-circles-directive", [])

.directive("packedCircles", ["d3Service", function(d3Service) {
	return {
		restrict: "E",
		scope: {
			vizData: "=",
            canvasWidth: "=",
            canvasHeight: "=",
            padding: "=",
            colorRange: "="
		},
        template: "<p>{{ title }}</p>",
		link: function(scope, element, attrs) {
			
			// get d3 promise
			d3Service.d3().then(function(d3) {
                                
                // set sizes from attributes in html element
                // if not attributes present - use default
				var width = parseInt(attrs.canvasWidth) || 700;
                var height = parseInt(attrs.canvasHeight) || width;
                var padding = parseInt(attrs.padding) || 15;
                var radius = (Math.min(width, height) - (padding * 2)) / 2;
                var diameter = radius * 2;
                
                // extra work to get a color array from an attribute
                // replace value commas with a pipe character so when we split later rgb values don't get broken
                // and replace quotes with nothing so our values can be consumed by d3
                var colorRange = attrs.colorRange ? attrs.colorRange.substring(1, attrs.colorRange.length - 1).replace(/',(\s+)?'/g,"|").replace(/'/g, "").split("|") : undefined || ["black", "grey", "darkgrey"];
                
                // set color
                var color = d3.scale
                    .linear()
                    .domain([-1, 5])
                    .range(colorRange)

                // create svg canvas
                var canvas = d3.select(element[0])
                    .append("svg")
                    .attr({
                        viewBox: "0 0 " + width + " " + height
                    })
                
                    // add group for slices
                    .append("g")
                    .attr({
						id: "packed-circles",
                        transform: "translate(" + radius + "," + radius + ")"
                      });
                
                // set layout
                var pack = d3.layout
                    .pack()
                    .size([diameter, diameter])
                    .value(function(d) { return d.size; });
                
                // bind data
                scope.$watch("vizData", function(newData, oldData) {
                    console.log("watch triggered");
                    
                    // async check
                    if (newData !== oldData) {
                        // remove all nodes
                            canvas
                                .selectAll("*")
                                .remove();
                        
                        draw(newData);
                        
                        function draw(data) {

                            // set variables to use for zooming and data starting point (a.k.a. root)
                            var focus = data;
                            var nodes = pack.nodes(data);
                            var view;

                            // set selection
                            var circle = d3.select("#packed-circles")
                                .selectAll("circle")
                                .data(nodes);

                            // enter selection
                            circle
                                .enter()
                                .append("circle");

                            // update selection
                            circle
                                .transition()
                                .duration(1000)
                                .attr({
                                    id: function(d) { return d.name + "-cp"; },
                                    class: function(d) { return d.parent ? d.children ? "node parent" : "node leaf" : "node root"; }
                                })
                                .style({
                                    fill: function(d) { return d.children ? color(d.depth) : null; }
                                })

                            // set events
                            circle
                                .on({
                                    click: function(d) { 

                                        // set viz name for title
                                        scope.title = d.name;

                                        // toggle css class to change state
                                        angular.element(this).toggleClass("active");

                                        // wrap element in angular(element) so we can toggle classes in other viz
                                        // need to make smarter like a service so directives don't have to know about
                                        // other existing directives

                                        // check for root
                                        if (!angular.element(this).hasClass("root")) {

                                            var hNode = document.getElementById(this.id.split("-")[0] + "-tl").parentNode;
                                            var el = angular.element(hNode);

                                            // toggle tree list item
                                            el.toggleClass("active");
                                            el.toggleClass("collapsed");

                                            // uncollapse up the tree
                                            stepThrough(d);

                                            function stepThrough(node) {

                                                // parent node
                                                var parent = node.parent;

                                                // uncollapse
                                                console.log(parent.name + " " + parent.depth);

                                                // check depth
                                                if (parent.depth > 1) {

                                                    // recurse nodes
                                                    stepThrough(parent);

                                                };

                                                return;

                                            };

                                        };

                                        // need to toggle zoom too
                                        if (focus !== d) {

                                            zoom(d);

                                            d3.event.stopPropagation(); 

                                        };

                                    }
                                });

                            // exit selection
                            circle
                                .exit()
                                .remove();

                            // set selection
                            var text = d3.select("#packed-circles")
                                .selectAll("text")
                                .data(nodes);

                            // enter selection
                            text
                                .enter()
                                .append("text");

                            // update selection
                            text
                                .transition()
                                .duration(1000)
                                .style({
                                    "fill-opacity": function(d) { return d.parent === data ? 1 : 0; },
                                    display: function(d) { return d.parent === data ? "inline" : "none"; }
                                })
                                .text(function(d) { return d.name; });

                            // exit selection
                            text
                                .exit()
                                .remove();

                            // set node variable for zooming capability
                            var node = canvas.selectAll("circle, text");

                            // attach event to svg and reset zoom
                            d3.select("packed-circles")
                                .select("svg")
                                .on("click", function() { 

                                    // reset zoom
                                    zoom(data); 

                                });

                            // set initial zoom
                            zoomTo([data.x, data.y, data.r * 2 + padding]);

                            function zoom(d) {

                                var focus0 = focus; focus = d;

                                var transition = d3.transition()
                                    .duration(d3.event.altKey ? 7500 : 750)
                                    .tween("zoom", function(d) {


                                        var i = d3.interpolateZoom(view, [focus.x, focus.y, focus.r * 2 + padding]);

                                        return function(t) { zoomTo(i(t)); };

                                });

                                transition
                                    .selectAll("text")
                                    .filter(function(d) { return d.parent === focus || this.style.display === "inline"; })
                                    .style("fill-opacity", function(d) { return d.parent === focus ? 1 : 0; })
                                    .each("start", function(d) { if (d.parent === focus) this.style.display = "inline"; })
                                    .each("end", function(d) { if (d.parent !== focus) this.style.display = "none"; });

                            };

                            function zoomTo(v) {

                                var k = diameter / v[2]; view = v;
                                node.attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1]) * k + ")"; });
                                circle.attr("r", function(d) { return d.r * k; })

                            };

                        };

                        // check new vs old
                        /*var isMatching = angular.equals(newData, oldData);console.log(isMatching);

                        // if false
                        if (!isMatching) {

                            // update the viz
                            draw(newData);

                        } else {
                            
                            // set the title
                            scope.title = newData.name;

                            // initial render
                            draw(newData);
                            
                        };*/
                        
                    };
                    
                });
				
			});
			
		}
		
	};
}]);