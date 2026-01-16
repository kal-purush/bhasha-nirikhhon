function lexer(code) {
    return code.split(/\s+/)
        .filter(function (t) { return t.length > 0 })
        .map(function (t) {
            return isNaN(t)
                ? { type: 'word', value: t }
                : { type: 'number', value: t }
        });
}

function parser(tokens) {
    var AST = {
        type: 'Drawing',
        body: []
    }
    // extract a token at a time as current_token. Loop until we are out of tokens.
    while (tokens.length > 0) {
        var current_token = tokens.shift()

        // Since number token does not do anything by it self, we only analyze syntax when we find a word.
        if (current_token.type === 'word') {
            switch (current_token.value) {
                case 'Paper':
                    var expression = {
                        type: 'CallExpression',
                        name: 'Paper',
                        arguments: []
                    }
                    // if current token is CallExpression of type Paper, next token should be color argument
                    var argument = tokens.shift()
                    if (argument.type === 'number') {
                        expression.arguments.push({  // add argument information to expression object
                            type: 'NumberLiteral',
                            value: argument.value
                        })
                        AST.body.push(expression)    // push the expression object to body of our AST
                    } else {
                        throw 'Paper command must be followed by a number.'
                    }
                    break
                case 'Pen':
                    var expression = {
                        type: 'CallExpression',
                        name: 'Pen',
                        arguments: []
                    }
                    // if current token is CallExpression of type Paper, next token should be color argument
                    var argument = tokens.shift()
                    if (argument.type === 'number') {
                        expression.arguments.push({  // add argument information to expression object
                            type: 'NumberLiteral',
                            value: argument.value
                        })
                        AST.body.push(expression)    // push the expression object to body of our AST
                    } else {
                        throw 'Pen command must be followed by a number.'
                    }
                    break
                case 'Line':
                    var expression = {
                        type: 'CallExpression',
                        name: 'Line',
                        arguments: []
                    }
                    // if current token is CallExpression of type Paper, next token should be color argument
                    var argument = tokens.shift()
                    if (argument.type === 'number') {
                        expression.arguments.push({  // add argument information to expression object
                            type: 'NumberLiteral',
                            value: argument.value
                        })
                        AST.body.push(expression)    // push the expression object to body of our AST
                    } else {
                        throw 'Line command must be followed by a number.'
                    }
                    break
            }
        }
    }
    return AST
}

console.log(parser(lexer("Paper 0\n Pen 100 \n Line 0 50 100 50")));

//console.log(lexer("Paper 0 \n Pen 100 \n Line 0 50 100 50"));