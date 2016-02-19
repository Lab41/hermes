angular.module("drag-drop-list-directive", [])

.directive("dragDropList", ["$timeout", function($timeout) {
	return {
		restrict: "E",
		scope: {
            formData: "="
        },
        templateUrl: "templates/drag-drop-list.html",
        controller: function($scope, $element, $attrs) {
            
        },
        link: function(scope, element, attrs) {
            
            // source item 
            var dragSrcEl = null;
            
            // bind events
            
            // drag enter
            element.on("dragenter", function(event) {
                
                // check siblings
                if (isBefore(dragSrcEl, event.target)) {
                    
                    // change the order
                    event.target.parentNode.insertBefore(dragSrcEl, event.target);
                    
                } else {
                    
                    // change the order
                    event.target.parentNode.insertBefore(dragSrcEl, event.target.nextSibling);
                    
                };
                
            });
            
            // drag start
            element.on("dragstart", function(event) {
                
                // remove drop style if it's there
                angular.element(event.target).removeClass("drop");
                
                // add drag style
                angular.element(event.target).addClass("drag");
                
                dragSrcEl = event.target;
                
                // swap HTML content
                event.dataTransfer.effectAllowed = "move";
                event.dataTransfer.setData("text/html", event.target.innerHTML);
                
            });
            
            // drag over
            element.on("dragover", function(event) {
                
                // if a dragged item has content in it that a browser
                // wants to interpret to do something stop it from
                // happening (like clicking through to a link)
                if (event.preventDefault) {
                    event.preventDefault();
                };
                
                event.dataTransfer.dropEffect = "move";
                
                // add style
                angular.element(event.target).addClass("drag-over");
                                
                return false;
                
            });
            
            // drag leave
            element.on("dragleave", function(event) {
                
                // remove style
                angular.element(event.target).removeClass("drag-over");
                
            });
            
            // drag end
            element.on("dragend", function(event) {
                
                // remove drag style
                angular.element(event.target).removeClass("drag");
                
            });
            
            // drop
            element.on("drop", function(event) {
                
                // prevent browser default actions on the drop
                // like a redirect which can be typical with a drop element
                if (event.stopPropagation) {
                    event.stopPropagation();
                };
                
                // check for drop into same drag element
                if (dragSrcEl != event.target) {
                    
                    // set source element content to html of dropped on
                    dragSrcEl.innerHTML = event.target.innerHTML;
                    event.target.innerHTML = event.dataTransfer.getData("text/html");
                    
                }
                
                // add style
                angular.element(event.target).addClass("drop");
                
                // remove style
                angular.element(event.target).removeClass("drag-over");
                
                // timed remove drop style to return to base style
                 $timeout(function() {
        
                    angular.element(event.target).removeClass("drop");;

                }, 1000);
                
                return false;
                
            });
            
            function isBefore(a, b) {
                
                // check if siblings
                if (a.parentNode == b.parentNode) {
                    
                    // loop through siblings
                    for (var i = a; i; i = i.previousSibling) {
                        
                        if (i === b) {
                            
                            return true;
                            
                        };
                        
                    };
                    
                };
                
                return false;
                
            };

		}
		
	};
    
}]);