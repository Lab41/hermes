angular.module("viz-controller", [])

.controller("vizCtrl", ["$scope", "dataService", function($scope, dataService) {
    
    /**************************/
    /********* !DATA **********/
    /**************************/
    
    $scope.scatterData;
	$scope.radarData;
	$scope.parallelData;
    
    getStatic("scatter", "combined_results"); // get formatted for scatter plot
    getStatic("parallel", "combined_results", { key: "structure", value: "parallel" }); // get for parallel coordinates plot
    
    /****************************/
    /********* !EVENTS **********/
    /****************************/
    
    /*******************************/
    /********* !FUNCTIONS **********/
    /*******************************/
    
    // viz data
	function getStatic(format, name, query) {
		dataService.getStatic(name, query).then(function(data) {
                        
            // assign to scope
            $scope[format + "Data"] = data;
            
		});
		
	};
	
}]);