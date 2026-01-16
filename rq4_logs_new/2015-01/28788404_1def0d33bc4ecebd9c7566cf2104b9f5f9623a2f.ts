import validation = require('./validation');

var validators:{ [x: string]: validation.StringValidator } = {};
validators['Min Length'] = new validation.MinLengthValidator();
validators['Letters Only'] = new validation.LettersOnlyValidator();
['ddddd', 'ss', '23sss'].forEach(s => {
    for (var v in validators) {
        console.log(s + ' ' + v + ' ' + (validators[v].isAcceptable(s) ? 'ok' : 'bad'));
    }
});
