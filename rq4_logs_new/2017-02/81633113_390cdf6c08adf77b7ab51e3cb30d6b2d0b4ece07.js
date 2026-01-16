var controllerService = SYMPHONY.services.register("symphonyAnalytics:controller");

SYMPHONY.remote.hello().then(function(data) {

    SYMPHONY.application.register("hello", ["modules", "applications-nav"], ["symphonyAnalytics:controller"]).then(function(response) {

        // The userReferenceId is an anonymized random string that can be used for uniquely identifying users.
        // The userReferenceId persists until the application is uninstalled by the user. 
        // If the application is reinstalled, the userReferenceId will change.
        var userId = response.userReferenceId;

        // Subscribe to Symphony's services
        var modulesService = SYMPHONY.services.subscribe("modules");
        var navService = SYMPHONY.services.subscribe("applications-nav");

        // LEFT NAV: Add an entry to the left navigation for our application
        navService.add("hello-nav", "Hello World App", "symphonyAnalytics:controller");

        // SHARE: Set the controller that implements the "link" method invoked when shared articles are clicked on.
        shareService.handleLink("article", "symphonyAnalytics:controller");

        // Implement some methods on our local service. These will be invoked by user actions.
        controllerService.implement({

            // LEFT NAV & MODULE: When the left navigation item is clicked on, invoke Symphony's module service to show our application in the grid
            select: function(id) {
                if (id == "hello-nav") {
                   // Focus the left navigation item when clicked
                    navService.focus("hello-nav"); 
                }
                
                modulesService.show("hello", {title: "Hello World App"}, "symphonyAnalytics:controller", "https://symphony-ss.domo.com/", {
                    // You must specify canFloat in the module options so that the module can be pinned
                    "canFloat": true,
                });
                // Focus the module after it is shown
                modulesService.focus("hello");
            },
        });
    }.bind(this))
}.bind(this));