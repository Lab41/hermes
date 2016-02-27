angular.module("data-service", [])

.factory("dataService", ["$http", function($http) {
	
    var urlBase="/rest/";
    var dataService = {};

	// get data
    dataService.getStatic = function(format, name) {
        
        var apiUrl = urlBase + "static/"

            apiUrl += name;

            
        // call data
        return $http.get(apiUrl).then(function(data) {
            
            // return data
            return data.data;
            
        });
		
    };
    
    return dataService;

}]);