var app = angular.module("app", [
    "ui.router",
    "hermes.controllers",
    "hermes.directives",
    "hermes.services",
    "hermes.filters"
]);

/***********************/
/********* RUN *********/
/***********************/

app.run(function() {
    
});

/**************************/
/********* CONFIG *********/
/**************************/

app.config(function($stateProvider, $urlRouterProvider) {

	/****************/
	/**** ROUTES ****/
	/****************/

	$stateProvider
    
    // main app (shared structure)
    .state("app", {
        url: "/",
        abstract: true,
        templateUrl: "templates/main.html",
        controller: "mainCtrl"
    })
    
    // explore vs explain
    .state("app.viz", {
        url: "{type}?{groupby}",
        templateProvider: function($http, $stateParams) {
            return $http.get("templates/" + $stateParams.type + ".html").then(function(template) {
                return template.data;
            });
        },
        controller: "vizCtrl"
    })

	// hardcoded for now need to create service
    $urlRouterProvider.otherwise("/scatter?groupby=algorithm");

});