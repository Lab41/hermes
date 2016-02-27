angular.module("module-title-directive", [])

.directive("moduleTitle", ["dataService", "$stateParams", "$state", "$rootScope", function(dataService, $stateParams, $state, $rootScope) {
	return {
		restrict: "E",
		scope: {
            meta: "=",
            allModules: "="
        },
        templateUrl: "templates/directives/module-title.html",
        controller: function($scope, $element, $attrs) {
			
			// parent scopes
			var mainScope = $scope.$parent.$parent.$parent;
            var vizScope = $scope.$parent.$parent;
            
            $scope.canBeReset = $stateParams.tool;
            
            // title
            $scope.isActive = false;

            // select title dropdown
            $scope.activeButton = function(e) {
                $scope.isActive = !$scope.isActive;
                e.stopPropagation();
            };
            
            // change the tool data
            $scope.changeTool = function(idx, parentIdx) {
                
                // detect if choosing a commit or a tool
                // change commit
                if ($attrs.meta == "commit") {
                    
                    // set current index
                    $scope.currentIndex = idx;
                    $scope.currentParentIndex = parentIdx;
                    
                    // selected module
                    var selectedModule = $scope.allModules[parentIdx].subItems[idx];
					
					// because commit is reset, get default items
					dataService.getItems($state.params.tool, selectedModule.id, "flare").then(function(data) {

						// item attributes for URL
						var itemString = data.name;
                        
                        // set params for use
                        mainScope.urlParams = {
                            type: $state.params.type,
                            sections: mainScope.urlParams.sections,
                            tool: $state.params.tool,
                            commit: selectedModule.id,
                            items: itemString,
							connection: "internal",
                            status: "all",
                            host: "all",
                            services: "all"
                        };
                                               
						// change url state
						$state.go($state.$current.name, mainScope.urlParams, {
                            notify: false,
                            reload: false
                        });
                        
                        // check state
                        if ($state.$current.name == "app.dashboard.detail") {
                            
                            // set commit scope
                            $scope.$parent.commit = selectedModule;
                            
                        } else {
                        
                            // set commit scope
                            vizScope.commit = selectedModule;
                            
                        };

					});
                   
                // change tool
                } else {
                    
                    // selected module
                    var selectedModule = $scope.allModules[idx];
                    
                    // get params for state change
                    dataService.getAttrs("commits", selectedModule.name).then(function(data) {

                        // current commit
                        var commit = data[0];
						
						// because tool is reset, get default items
						dataService.getItems(selectedModule.name, commit.id, "flare").then(function(data) {

							// item attributes for URL
							var itemString = data.name;
							
							// set params for use
							mainScope.urlParams = {
                                type: $state.params.type,
                                sections: mainScope.urlParams.sections,
								tool: selectedModule.name,
								commit: commit.id,
								items: itemString,
								connection: "internal",
                                status: "all",
                                host: "all",
                                services: "all"
							};
							
							// change url state
							$state.go($state.$current.name, mainScope.urlParams);
                            
                            // reset heatmap b/c it needs to be redrawn
                            //vizScope.heatChartExists = false;
							
							// set tool info
                        	mainScope.tool = selectedModule;
                            
                            // set current commit
                            vizScope.commit = commit;
							
						});

                    });
                    
                };
                 
            };
            
            // reset tool
            $scope.reset = function(tool) {
                
                console.log("need to reset " + tool);
                
            };
            
            // click away or esc to close drop down
            $rootScope.$on("documentClicked", _close);
            $rootScope.$on("escapedPressed", _close);

            function _close() {
                $scope.$apply(function() {
                    $scope.isActive = false;
                });
            };
        
        },
        link: function(scope, element, attrs) {
            
            scope.$watch("allModules", function(newData, oldData) {
                
                // parent scopes
                var vizScope = scope.$parent.$parent;
                
                // async check
                if (newData !== undefined) {
                    //console.log("data is ready");

                    // check new vs old
                    var isMatching = angular.equals(newData, oldData);

                    // if false
                    if (!isMatching) {

                        // set index to apply styling
                        angular.forEach(newData, function(value, key) {

                            // check for subitems
                            var isNest = Object.keys(value).indexOf("subItems") == -1 ? false : true;

                            if (isNest == true) {
                                
                                angular.forEach(value.subItems, function(value2, key2) {

                                    if (value2.id == vizScope.commitId) {
                                        
                                        scope.currentIndex = key2;
                                        scope.currentParentIndex = key;

                                    };

                                });
                                
                            };
                            
                        });

                    };
                    
                };
                        
            });

		}
		
	};
    
}]);
