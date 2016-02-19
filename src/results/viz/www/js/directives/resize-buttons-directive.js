angular.module("resize-buttons-directive", [])

.directive("resizeButtons", ["$state", "$stateParams", function($state, $stateParams) {
	return {
		restrict: "E",
		scope: {
			sectionName: "="
		},
        templateUrl: "templates/resize-buttons.html",
        controller: function($scope, $state, $stateParams, $attrs) {
			
			// set button visibility
			$scope.setVisible = $state.$current.name == 'app.dashboard.set' ? true : false;
			$scope.detailVisible = $state.$current.name == 'app.dashboard.detail' ? true : false;
				
			// expand event
			$scope.expand = function() {
			
				// hide set
				$scope.$parent.$parent.isVisible = false;

				// show single
				$scope.$parent.$parent.$$nextSibling.isVisible = true;
				
				// change state
				$state.go("app.dashboard.detail", {
					type: $stateParams.type,
					id: $attrs.sectionName
				});
				
			};
			
			// close event
			$scope.close = function() {
			
				// hide detail
				$scope.$parent.$parent.isVisible = false;

				// hide set
				$scope.$parent.$parent.$$prevSibling.isVisible = true;
				
				// change state
				$state.go("app.dashboard.set", {
					type: $stateParams.type
				});
				
			};
                 
        },
        link: function(scope, element, attrs) {

		}
		
	};
    
}]);