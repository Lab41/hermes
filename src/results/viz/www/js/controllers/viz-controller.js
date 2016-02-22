angular.module("viz-controller", [])

.controller("vizCtrl", ["$scope", "dataService", function($scope, dataService) {
    
    /**************************/
    /********* !DATA **********/
    /**************************/
    
    $scope.axis_mapData;
    $scope.combined_resultsData;
    $scope.combined_resultsDataNest;
    
    getStatic(null, "combined_results", "csv"); // get chart data
    getStatic(null, "axis_map", "json"); // get labels
    getStatic("nest", "combined_results", "csv"); // gest nested data TODO make REST smarter
    
    /****************************/
    /********* !EVENTS **********/
    /****************************/
    
    /*******************************/
    /********* !FUNCTIONS **********/
    /*******************************/
    
    // viz data
	function getStatic(format, name, ext) {
		dataService.getStatic(format, name, ext).then(function(data) {
            
            // check format
            if (format == null) {
                        
                // assign to scope
                $scope[name + "Data"] = data;
                
            } else {
                
                // assign to scope
                $scope[name + "DataNest"] = data;
                
            };
            
		});
		
	};
	
}]);