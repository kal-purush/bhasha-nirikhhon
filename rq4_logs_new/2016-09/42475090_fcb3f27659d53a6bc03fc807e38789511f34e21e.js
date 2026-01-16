(function(global, $) {

    var Greetr = function (firstName, lastName, language) {
        return new Greetr.init(firstName, lastName, language);
    };

    var supportedLangs = ['en', 'es'];

    var greetings = {
        en: "Hello",
        es: "Hola"
    };

    var formalGreetings = {
        en: "Greetings",
        es: "Saludos"
    };

    var logMessages = {
        en: 'Logged in',
        es: 'Inicio sesion'
    };

    Greetr.prototype = {
        fullName: function() {
            return this.firstName + " " + this.lastName;
        },

        validate: function() {
            if(supportedLangs.indexOf(this.language) === 1) {
                throw "Invalid language"
            }
        },

        greeting: function() {
            return greetings[this.language] + this.firstName + "!"
        },

        formalGreeting: function () {
            return formalGreetings[this.language] + ", " + this.fullName();
        }


    };

    Greetr.init = function(firstName, lastName, language) {

        var self = this;

        self.firstName = firstName || "";
        self.lastName = lastName || "";
        self.language = language || "en";
    };

    Greetr.init.prototype = Greetr.prototype;

    global.Greetr = global.G$ = Greetr;

})(window, jQuery);