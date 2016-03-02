angular.module("data-service", [])

.factory("dataService", ["$http", "$stateParams", function($http, $stateParams) {
	
    var urlBase="/rest/";
    var dataService = {};

	// get data
    dataService.getStatic = function(name, query) {
        
        var apiUrl = urlBase + "static/"

		// check query
		if (query != null) {
            
            // check for parallel TODO clean up make more modular
            if (query.value == "parallel") {
                
                apiUrl += name + "?structure=parallel&dimensions=" + $stateParams.dimensions;
                
            } else {
			
			     apiUrl += name + "?" + query.key + "=" + query.value;
                
            };
			
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