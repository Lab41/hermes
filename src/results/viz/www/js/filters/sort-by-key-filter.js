angular.module("sorybykey-filter", [])

.filter("sorybykey", function() {
	
	return function(input) {
		
		var filtered = [];
		
		for (var i=0; i < input.length; i++) {
			
			var item = input[i];
			
			if (/a/i.test(item.substring(0, 1))) {
				
				filtered.push(item);
				
			};
			
		};
		
		return filtered;
				
	};
	
});