angular.module("viz-controller", [])

.controller("vizCtrl", ["$scope", "dataService", function($scope, dataService) {
    
    /**************************/
    /********* !DATA **********/
    /**************************/
    
    $scope.axis_mapData;
    $scope.combined_resultsData;
    
    getStatic("combined_results", "csv"); // get chart data
    getStatic("axis_map", "json"); // get labels
    
    /****************************/
    /********* !EVENTS **********/
    /****************************/
    
    /*******************************/
    /********* !FUNCTIONS **********/
    /*******************************/
    
    // viz data
	function getStatic(name, ext) {
		dataService.getStatic(name, ext).then(function(data) {
                        
            // assign to scope
			$scope[name + "Data"] = data;
            
		});
		
	};
	
}]);