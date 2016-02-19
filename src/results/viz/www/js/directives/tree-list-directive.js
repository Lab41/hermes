angular.module("tree-list-directive", [])

// tree
.directive("treeList", [function() {
	return {
		restrict: "E",
		template: "<ul><branch ng-repeat='c in vizData.children' viz-data='c'></branch></ul>",
		scope: {
			vizData: "="
		}
	};
    
}])

// branch
.directive("branch", function($compile) {
    return {
        restrict: "E",
        replace: true,
        scope: {
            branch: "=vizData"
        },    
        template: "<li class='collapsed'><a id='{{ branch.name }}-tl'>{{ branch.name }}</a></li>",
        link: function(scope, element, attrs) {
            
            // check if there are any children, otherwise we'll have infinite execution
            var has_children = angular.isArray(scope.branch.children);

            // check for children
            if (has_children) {
                
                // add branch
                element.append("<tree-list viz-data='branch'></tree-list>");

                // recompile Angular because of manual appending
                $compile(element.contents())(scope); 
                
            };

            // bind events
            element.on("click", function(event) {
            
                event.stopPropagation();
                
                // check for children
                if (has_children) {
                    
                    // toggle css class to change state
                    element.toggleClass("collapsed");
                    element.toggleClass("active");
                    
                    // wrap element in angular(element) so we can toggle classes in other viz
                    // need to make smarter like a service so directives don't have to know about
                    // other existing directives
                    var el = angular.element(document.getElementById(event.target.id.split("-")[0] + "-cp"));
                    
                    // toggle it
                    el.toggleClass("active"); 
                    
                };

            });     
            
        }
        
    };
    
});