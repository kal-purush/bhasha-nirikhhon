export class FacebookService{

    constructor(Facebook, $q, $log) {
        'ngInject';

        this.facebook = Facebook;
        this.$q = $q;
        this.$log = $log;
    }

    login(scope){
        var deferred = this.$q.defer();
        this.$log.info('facebook logging in');
        this.facebook.login((response)=>{
            this.$log.info(response);
            deferred.resolve(response);
        }, scope);

        return deferred.promise;
    }

    me(){
        var deferred = this.$q.defer();
        this.facebook.api('/me?locale=en_US&fields=name,email', function(response){
            deferred.resolve(response);
        });

        return deferred.promise;
    }
}