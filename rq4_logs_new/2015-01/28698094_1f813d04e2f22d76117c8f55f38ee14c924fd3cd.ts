function LSystem() {
    this.rules = [];
}

var rulePattern = /^((\w*)<)?(\w*)(\((\d*\.?\d*)\))?(>(\w*))?$/;
LSystem.prototype.addRule = function(rule, result) {
    var parts = rulePattern.exec(rule);
    if (parts) {
        this.rules.push({
            key: parts[3],
            pre: parts[2],
            post: parts[7],
            chance: parseFloat(parts[5]),
            result: result
        });
    }

    return this;
}

LSystem.prototype.findRule = function(cpre, c, cpost) {
    // reverse order as later rules are more specific
    for (var i = this.rules.length - 1; i >= 0; --i) {
        var rule = this.rules[i];
        if (rule.pre && rule.pre !== cpre)
            continue;

        if (rule.post && rule.post !== cpost)
            continue;

        if (rule.key && rule.key !== c)
            continue;

        return rule;
    }

    return null;
}

LSystem.prototype.iterate = function(str) {
    if (str === '')
        return '';

    var newStr = '',
        cpre = '',
        c = '',
        cpost = str[0];

    for (var i = 0; i < str.length; ++i) {
        cpre = c;
        c = cpost;
        cpost = str[i + 1];

        var rule = this.findRule(cpre, c, cpost);

        if (rule && rule.chance && Math.random() >= rule.chance)
            rule = null; // failed chance test, invalidate the rule

        newStr += (rule ? rule.result : c);
    }

    return newStr;
}

LSystem.prototype.join = function(separator) {
    var str = '';
    for (var i = 0; i < this.rules.length; ++i) {
        if (i > 0)
            str += separator;

        var rule = this.rules[i];
        if (rule.pre)
            str += rule.pre + '<';
        if (rule.key)
            str += rule.key
        if (rule.chance)
            str += '(' + rule.chance.toString() + ')';
        if (rule.post)
            str += '>' + rule.post;

        str += ' : ' + rule.result;
    }
    return str;
}