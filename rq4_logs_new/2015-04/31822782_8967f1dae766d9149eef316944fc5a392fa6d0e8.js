/**
 * Created by root on 3/30/15.
 */

define([], function() {

    function HomeController() {

    }

    HomeController.prototype.start = function() {
        console.log("inside home controller");
        $('#toForm').on('click', function() {
            window.location = '/form';
        });

        $('#toAccount').on('click', function() {
           console.log("Go to my account");
        });

    };

    return HomeController;
});