angular.module("viz-controller", [])

.controller("vizCtrl", ["$scope", "dataService", function($scope, dataService) {
    
    /**************************/
    /********* !DATA **********/
    /**************************/
    
    $scope.flareData;
    $scope.nodeLinkData;
    
    /****************************/
    /********* !EVENTS **********/
    /****************************/
    
    /*******************************/
    /********* !FUNCTIONS **********/
    /*******************************/
    
    // get data to use in visualizations
    getData("flare");
    getData("nodeLink");
    
    // viz data
	function getData(name) {
		dataService.getData(name).then(function(data) {
                        
            // assign to scope
			$scope[name + "Data"] = data;
            
		});
		
	};
	
}]);