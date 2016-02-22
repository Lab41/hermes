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
            $scope.axis = 13;
            
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
				var rScale = d3.scale.linear()
                    .domain([0, 0.5])
					.range([0, radius]);
				
				/*var yScale = d3.scale.linear()
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
					.orient("left");*/
												
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
                            
                            // attribute axis
                            var aAxis = canvas
                                .append("g")
                                .attr({
                                    class: "a-axis"
                                })
                                .selectAll("g")
                                .data(d3.range(0, 360, (360 / radialCount)))
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
                            
                            /*var series, 
   hours,
    minVal,
    maxVal,
    w = 400,
    h = 400,
    vizPadding = {
        top: 10,
        right: 0,
        bottom: 15,
        left: 0
    },
    radiusLength,
    ruleColor = "#CCC";*/

  loadData();
  //setScales();
  //addAxes();
  //draw();

function loadData(){
    var randomFromTo = function randomFromTo(from, to){
       return Math.floor(Math.random() * (to - from + 1) + from);
    };

    series = [
      [],
      []
    ];

    hours = [];

    for (i = 0; i < 24; i += 1) {
        series[0][i] = randomFromTo(0,20);
        series[1][i] = randomFromTo(5,15);
        hours[i] = i; //in case we want to do different formatting
    }

    mergedArr = series[0].concat(series[1]);

    minVal = d3.min(mergedArr);
    maxVal = d3.max(mergedArr);
    //give 25% of range as buffer to top
    maxVal = maxVal + ((maxVal - minVal) * 0.25);
    minVal = 0;

    //to complete the radial lines
    for (i = 0; i < series.length; i += 1) {
        series[i].push(series[i][0]);
    }
};

function setScales() {
  var heightCircleConstraint,
      widthCircleConstraint,
      circleConstraint,
      centerXPos,
      centerYPos;

  //need a circle so find constraining dimension
  heightCircleConstraint = h - vizPadding.top - vizPadding.bottom;
  widthCircleConstraint = w - vizPadding.left - vizPadding.right;
  circleConstraint = d3.min([
      heightCircleConstraint, widthCircleConstraint]);

  radius = d3.scale.linear().domain([minVal, maxVal])
      .range([0, (circleConstraint / 2)]);
  radiusLength = radius(maxVal);

  //attach everything to the group that is centered around middle
  centerXPos = widthCircleConstraint / 2 + vizPadding.left;
  centerYPos = heightCircleConstraint / 2 + vizPadding.top;

  vizBody.attr("transform",
      "translate(" + centerXPos + ", " + centerYPos + ")");
};

function addAxes () {
  var i,
      circleAxes,
      lineAxes;

  circleAxes = vizBody.selectAll('.circle-ticks')
      .data([1,1,1,1,1])
      .enter().append('svg:g')
      .attr("class", "circle-ticks");

  circleAxes.append("svg:circle")
      .attr("r", function (d, i) {
          return radius(d);
      })
      .attr("class", "circle")
      .style("stroke", ruleColor)
      .style("fill", "none");

  circleAxes.append("svg:text")
      .attr("text-anchor", "middle")
      .attr("dy", function (d) {
          return -1 * radius(d);
      })
      .text(String);

  lineAxes = vizBody.selectAll('.line-ticks')
      .data(hours)
      .enter().append('svg:g')
      .attr("transform", function (d, i) {
          return "rotate(" + ((i / hours.length * 360) - 90) +
              ")translate(" + radius(maxVal) + ")";
      })
      .attr("class", "line-ticks");

  lineAxes.append('svg:line')
      .attr("x2", -1 * radius(maxVal))
      .style("stroke", ruleColor)
      .style("fill", "none");

  lineAxes.append('svg:text')
      .text(String)
      .attr("text-anchor", "middle")
      .attr("transform", function (d, i) {
          return (i / hours.length * 360) < 180 ? null : "rotate(180)";
      });
};

function draw () {
  var groups,
      lines,
      linesToUpdate;

  highlightedDotSize = 4;

  groups = vizBody.selectAll('.series')
      .data(series);
  groups.enter().append("svg:g")
      .attr('class', 'series')
      .style('fill', function (d, i) {
          if(i === 0){
            return "green";
          } else {
            return "blue";
          }
      })
      .style('stroke', function (d, i) {
          if(i === 0){
            return "green";
          } else {
            return "blue";
          }
      });
  groups.exit().remove();

  lines = groups.append('svg:path')
      .attr("class", "line")
      .attr("d", d3.svg.line.radial()
          .radius(function (d) {
              return 0;
          })
          .angle(function (d, i) {
              if (i === 24) {
                  i = 0;
              } //close the line
              return (i / 24) * 2 * Math.PI;
          }))
      .style("stroke-width", 3)
      .style("fill", "none");

  groups.selectAll(".curr-point")
      .data(function (d) {
          return [d[0]];
      })
      .enter().append("svg:circle")
      .attr("class", "curr-point")
      .attr("r", 0);

  groups.selectAll(".clicked-point")
      .data(function (d) {
          return [d[0]];
      })
      .enter().append("svg:circle")
      .attr('r', 0)
      .attr("class", "clicked-point");

  lines.attr("d", d3.svg.line.radial()
      .radius(function (d) {
          return radius(d);
      })
      .angle(function (d, i) {
          if (i === 24) {
              i = 0;
          } //close the line
          return (i / 24) * 2 * Math.PI;
      }));
};

                        };
                        
                    };
                    
                });       
                
            });
            
        }
    }
    
}]);