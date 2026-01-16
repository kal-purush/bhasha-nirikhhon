angular.module('easypoll')
    .service('contextService', function () {


        let context = Session.get('context');
        if(!context) {
            context={};
        }

        return {

            getContext: () => {

                return context;

            },
            saveContext: () => {

                Session.set('context', context);

            }
        }

    });
