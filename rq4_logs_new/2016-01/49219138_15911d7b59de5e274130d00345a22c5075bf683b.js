/*
Gumball
 */

(function(root, factory){

    if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory();
    } else {
        root['Gumball'] = factory();
    }

})(this, function() {

    var Gumball = {

        resource : {},
        method : {},

        addResource : function(namespace, name, obj) {
            if (!this.resource[namespace]) {
                this.resource[namespace] = {};
            }
            this.resource[namespace][name] = obj;
        },

        addMethod : function(methodName, cb) {
            this.method[methodName] = cb;
        }

    };

    return Gumball;

});