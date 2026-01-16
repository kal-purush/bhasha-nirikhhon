define(['app/config', 'app/utils'], function(config, utils) {

    return {

        url: 'https://api.enterprise.apigee.com/v1/o/' + config.org + '/developers/',

        /**
         * Get developer attributes
         */
        getDeveloper: function(params) {
            utils.submitRequest(this.url + params.email, 'GET');
        }

    }

});