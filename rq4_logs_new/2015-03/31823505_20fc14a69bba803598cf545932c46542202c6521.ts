module app.services
{
    export function FirebaseFactory ()
    {
        return new Firebase('https://crackling-fire-4047.firebaseio.com');
    }
    angular.module('app.services').factory('firebaseFactory', FirebaseFactory);
}