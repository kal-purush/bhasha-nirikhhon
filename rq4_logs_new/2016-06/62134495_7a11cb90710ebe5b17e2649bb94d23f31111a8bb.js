function DashboardController($state, logoutFactory) {
    var dc = this;

    dc.logoutButton = function() {
        logoutFactory.logout().then(function(response) {
            $state.go('login');
        });
    };
}

module.exports = DashboardController;