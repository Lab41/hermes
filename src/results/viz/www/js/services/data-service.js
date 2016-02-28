angular.module("data-service", [])

.factory("dataService", ["$http", function($http) {
	
    var urlBase="/rest/";
    var dataService = {};

	// get data
    dataService.getStatic = function(name, query) {
        
        var apiUrl = urlBase + "static/"

		// check query
		if (query != null) {
			
			apiUrl += name + "?" + query.key + "=" + query.value;
			
		} else {
			
            apiUrl += name;
			
		};

            
        // call data
        return $http.get(apiUrl).then(function(data) {
            
            // return data
            return data.data;
            
        });
		
    };
    
    return dataService;

}]);