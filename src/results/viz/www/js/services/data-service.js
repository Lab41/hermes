angular.module("data-service", [])

.factory("dataService", ["$http", function($http) {
	
    var urlBase="/rest/";
    var dataService = {};

	// get data
    dataService.getStatic = function(format, name, ext) {
        
        var apiUrl = urlBase
        
        // check format
        if (format != null) {
        
            apiUrl += "nest/" + name;
            
        } else {
            
            apiUrl += name;
            
        };
            
        // call data
        return $http.get(apiUrl + "?ext=" + ext).then(function(data) {
            
            // return data
            return data.data;
            
        });
		
    };
    
    return dataService;

}]);