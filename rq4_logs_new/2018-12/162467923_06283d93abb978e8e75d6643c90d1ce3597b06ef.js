'use strict';

console.log('Hello RulesEngineMath');

module.exports = {

    area: function (width, height) {
        return width * height;
    },
    fromSQLToJS: function (exp) {
        var regolaJS = null;
        try {

            regolaJS = exp;
            regolaJS = regolaJS.replace(/and/gi, " && ");
            regolaJS = regolaJS.replace(/or/gi, " || ");
            regolaJS = regolaJS.replace(/=/gi, " == ");
            regolaJS = " return " + regolaJS + " ; ";

        }
        catch (exception) {

            console.log(exception);
            regolaJS = null;

        }


        return regolaJS;

    }
};



 