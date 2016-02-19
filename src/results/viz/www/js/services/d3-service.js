angular.module("d3-service", [])

.factory("d3Service", ["$document", "$window", "$q", "$rootScope", function($document, $window, $q, $rootScope) {
	
	var d = $q.defer();
	var d3service = {
		d3: function() {
			return d.promise;
		}
	};
	
	//create script tag for d3 source
	var scriptTag = $document[0].createElement("script");
	scriptTag.type = "text/javascript";
	scriptTag.async = true;
	scriptTag.src = "https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js";
	scriptTag.onreadystatechange = function() {
		
		//check state
		if(this.readyState == "complete") {
			
			onScriptLoad();
			
		};
		
	};
	
	scriptTag.onload = onScriptLoad;
	
	//add script tage to document
	var s = $document[0].getElementsByTagName("body")[0];
	s.appendChild(scriptTag);
	
	//return d3 object
    return d3service;
	
	function onScriptLoad() {
		
		//load client in the browser
		$rootScope.$apply(function() {
			d.resolve($window.d3);
		});
		
	};
	
}]);