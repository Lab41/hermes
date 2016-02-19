angular.module("module-title-directive", [])

.directive("moduleTitle", ["dataService", function(dataService) {
	return {
		restrict: "E",
		scope: {
            meta: "=",
            allModules: "="
        },
        templateUrl: "templates/module-title.html",
        controller: function($scope, $element, $attrs) {
            
            // title
            $scope.isActive = false;

            // select title dropdown
            $scope.activeButton = function(e) {
                $scope.isActive = !$scope.isActive;
            };
            
            // change the viz data
            $scope.changeViz = function(idx, parentIdx) {
                console.log("change the viz");
                // selected module
                var selectedModule = $scope.allModules[parentIdx].subItems[idx];
                var selectedSubhead = $scope.allModules[parentIdx].title;
                
                // get module attributes to change to
                var structure = "flare";
                var className = "poll_data";
                var id = selectedModule.requestID;
                var tool = selectedModule.tool;
                var title = selectedModule.title;
                var start = null;
                var end = null;
                var heatStart = null;
                var heatEnd = null;
                
                // modify selection title so it fits in the HTML structure
                selectedModule["title"] = selectedSubhead + " " + title;
                
                // set meta info
                $scope.$parent.$parent.dataDate = selectedModule;
                
                // get data from API based off configs
                getData(structure, className, id, tool, null, null, tool, heatStart, heatEnd);
                
                // viz data
                function getData(structure, className, id, tool, start, end, toolName, heatStart, heatEnd) {
                    dataService.getData(structure, className, id, tool, start, end).then(function(data) {

                        var scopeName = "data" + structure.toUpperCase().substring(0, 1) + structure.substring(1, structure.length);
                        var scopeMain = $scope.$parent.$parent;

                        // viz data in flare format
                        scopeMain.dataFlare = data[structure];

                        // check data format
                        if (structure == "flare") {

                            // set the id for use in the heatmap grid
                            scopeMain.toolChildId = data[structure].name;

                            // get time data here because we need the async result of the child name
                            getData("time", "node_count", scopeMain.toolChildId, toolName, scopeMain.startDate, scopeMain.endDate);

                        };

                    });

                };
                console.log("change viz done"); 
            };
            
            // change the tool data
            $scope.changeTool = function(idx, parentIdx) {
                
                // selected module
                var selectedModule = $scope.allModules[parentIdx];
                
                // set tool info
                $scope.$parent.$parent.tool = selectedModule;
                $scope.$parent.$parent.toolName = selectedModule.title;
                 
            };
        
        },
        link: function(scope, element, attrs) {

		}
		
	};
    
}]);