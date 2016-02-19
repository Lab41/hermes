var app = angular.module("app", [
    "ui.router",
    "hermes.controllers",
    "hermes.directives",
    "hermes.services"
]);

/***********************/
/********* RUN *********/
/***********************/

app.run(function() {
    
    // run methods here
    
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
        url: "{type}",
        templateUrl: "templates/viz.html",
        controller: "vizCtrl"
    })

    $urlRouterProvider.otherwise("/explore");

});