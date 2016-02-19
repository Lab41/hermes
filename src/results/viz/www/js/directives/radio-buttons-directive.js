angular.module("radio-buttons-directive", [])

.directive("radioButtons", [function() {
	return {
		restrict: "E",
		scope: {
            formData: "="
        },
        templateUrl: "templates/radio-buttons.html",
        controller: function($scope, $element, $attrs) {
                 
        },
        link: function(scope, element, attrs) {

		}
		
	};
    
}]);