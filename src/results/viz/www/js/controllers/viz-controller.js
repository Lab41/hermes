angular.module("viz-controller", [])

.controller("vizCtrl", ["$scope", "dataService", "$stateParams", function($scope, dataService, $stateParams) {
    
    /**************************/
    /********* !DATA **********/
    /**************************/
    
    $scope.scatterData;
	$scope.parallelData;
    
    getStatic("scatter", "combined_results"); // get formatted for scatter plot
    getStatic("parallel", "combined_results", { key: "structure", value: "parallel" }, $stateParams.dimensions); // get for parallel coordinates plot
    
    /****************************/
    /********* !EVENTS **********/
    /****************************/
    
    /*******************************/
    /********* !FUNCTIONS **********/
    /*******************************/
    
    // viz data
	function getStatic(format, name, query, params) {
		dataService.getStatic(name, query, params).then(function(data) {
                        
            // assign to scope
            $scope[format + "Data"] = data;
            
		});
		
	};
	
}]);