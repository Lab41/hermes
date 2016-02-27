angular.module("viz-controller", [])

.controller("vizCtrl", ["$scope", "dataService", function($scope, dataService) {
    
    /**************************/
    /********* !DATA **********/
    /**************************/
    
    $scope.scatterData;
    
    getStatic("scatter", "combined_results"); // get scatter data
    
    /****************************/
    /********* !EVENTS **********/
    /****************************/
    
    /*******************************/
    /********* !FUNCTIONS **********/
    /*******************************/
    
    // viz data
	function getStatic(format, name) {
		dataService.getStatic(format, name).then(function(data) {
                        
            // assign to scope
            $scope[format + "Data"] = data;
            
		});
		
	};
	
}]);