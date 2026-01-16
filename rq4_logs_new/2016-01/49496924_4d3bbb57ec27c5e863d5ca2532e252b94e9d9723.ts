/**
 * Created by Irakli Zarandia on 1/5/2016.
 */

module Helpers
{
    export class ValueGenerator
    {
        /**
         *
         * @type {string[]}
         */
        private static UPPERS:  Array<string> = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"];

        /**
         *
         * @type {string[]}
         */
        private static LOWERS:  Array<string> = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"];

        /**
         *
         * @type {string[]}
         */
        private static DIGITS:  Array<string> = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

        /**
         *
         * @type {string[]}
         */
        private static SPACES:  Array<string> = [" ", "\n", "\t"];

        /**
         *
         * @type {string[]}
         */
        private static OTHERS:  Array<string> = ["!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "`", "{", "|", "}", "~"];

        /**
         *
         * @type {Array<string>[]}
         */
        private static ALL:     Array<string> = [].concat(ValueGenerator.UPPERS, ValueGenerator.LOWERS, ValueGenerator.DIGITS, " ", ValueGenerator.OTHERS, ["_"]);

        /**
         *
         * @type {{d: Array<string>, D: (T[]|Array<string>[]), w: (T[]|Array<string>[]), W: (T[]|Array<string>[]), t: string[], n: string[], v: string[], f: string[], r: string[], s: Array<string>, S: (T[]|Array<string>[]), 0: string[]}}
         */
        private static CLASSES: Object = {
            'd' : RegexValueGenerator.DIGITS,
            'D' : [].concat(RegexValueGenerator.UPPERS, RegexValueGenerator.LOWERS, RegexValueGenerator.SPACES, RegexValueGenerator.OTHERS, ['_']),
            'w' : [].concat(RegexValueGenerator.UPPERS, RegexValueGenerator.LOWERS, RegexValueGenerator.DIGITS, ['_']),
            'W' : [].concat(RegexValueGenerator.SPACES, RegexValueGenerator.OTHERS),
            't' : [ '\t' ],
            'n' : [ '\n' ],
            'v' : [ '\u000B' ],
            'f' : [ '\u000C' ],
            'r' : [ '\r' ],
            's' : RegexValueGenerator.SPACES,
            'S' : [].concat(RegexValueGenerator.UPPERS, RegexValueGenerator.LOWERS, RegexValueGenerator.DIGITS, RegexValueGenerator.OTHERS, ['_']),
            '0' : [ '\0' ]
        };

        /**
         *
         * @type {{}}
         * @private
         */
        private _reference: Object = {};

        /**
         *
         * @type {number}
         * @private
         */
        private _Counter: number = 1;

        /**
         *
         * @param pattern
         * @returns {Array<any>}
         */
        private processGrouping(pattern): Array<any>
        {
            var tree:  Array<any> = [];
            var stack: Array<any> = [tree];

            while(pattern.length)
            {
                var chr: string = pattern.shift();

                if(chr === '\\')
                {
                    var next: string = pattern.shift();

                    if(next === '(' || next === ')')
                    {
                        stack[0].push(next);
                    }
                    else
                    {
                        stack[0].push(chr, next);
                    }
                }
                else if (chr === '(')
                {
                    var inner: Array<any> = [];

                    stack[0].push(inner);
                    stack.unshift(inner);

                    var next: string = pattern.shift();

                    if(next === '?')
                    {
                        next = pattern.shift();
                        if(next !== ':')
                        {
                            throw "Invalid group";
                        }
                    }
                    else if (next === '(' || next === ')')
                    {
                        pattern.unshift(next);

                    }
                    else
                    {
                        inner[this._Counter] = this._Counter++;
                        inner.push(next);
                    }
                }
                else if (chr === ')')
                {
                    stack.shift();
                }
                else
                {
                    stack[0].push(chr);
                }
            }

            if(stack.length > 1)
            {
                throw "missmatch paren";
            }

            return tree;
        }

        /**
         *
         * @param tree
         * @returns {string}
         */
        private processOthers(tree): string
        {
            var ret:        string    = '';
            var candinates: any = [];
            var tree:       any       = tree.slice(0);

            /**
             *
             * @returns {*|string}
             */
            var choice: Function = ()=>
            {
                if(typeof candinates !== 'undefined')
                {
                    var ret = candinates[Math.floor(candinates.length * Math.random())];

                    if (ret instanceof Array)
                    {
                        ret = this.processOthers(ret);
                    }

                    if (candinates[this._Counter])
                    {
                        this._reference[candinates[this._Counter]] = ret;
                    }
                }

                return ret || '';
            };

            while (tree.length)
            {
                var chr: string = tree.shift();

                switch(chr)
                {
                    case '^':
                    case '$':
                        break;
                    case '*':
                        for(var i: number = 0, len: number = Math.random() * 10; i < len; i++)
                        {
                            ret += choice();
                        }

                        candinates = [];

                        break;
                    case '+':
                        for(var i: number = 0, len: number = Math.random() * 10 + 1; i < len; i++)
                        {
                            ret += choice();
                        }

                        candinates = [];

                        break;
                    case '{':
                        var brace: string = '';

                        while(tree.length)
                        {
                            chr = tree.shift();

                            if (chr === '}')
                            {
                                break;
                            }
                            else
                            {
                                brace += chr;
                            }
                        }

                        if (chr !== '}')
                        {
                            throw "missmatch brace: " + chr;
                        }

                        var dd = brace.split(/,/);
                        var min = +dd[0];
                        var max = (dd.length === 1) ? min : (+dd[1] || 10);

                        for(var i: number = 0, len: number = Math.floor(Math.random() * (max - min + 1)) + min; i < len; i++)
                        {
                            ret += choice();
                        }

                        candinates = [];

                        break;
                    case '?':
                        if (Math.random() < 0.5) {
                            ret += choice();
                        }
                        candinates = [];

                        break;

                    case '\\':
                        ret += choice();
                        var escaped = tree.shift();

                        if (escaped.match(/^[1-9]$/)) {
                            candinates = [ this._reference[escaped] || '' ];
                        } else {
                            if (escaped === 'b' || escaped === 'B') {
                                throw "\\b and \\B is not supported";
                            }
                            candinates = RegexValueGenerator.CLASSES[escaped];
                        }

                        if (!candinates)
                        {
                            candinates = [ escaped ];
                        }

                        break;
                    case '[':
                        ret += choice();

                        var sets = [];
                        var negative = false;

                        while (tree.length)
                        {
                            chr = tree.shift();
                            if (chr === '\\')
                            {
                                var next = tree.shift();

                                if(ValueGenerator.CLASSES[next])
                                {
                                    sets = sets.concat(ValueGenerator.CLASSES[next]);
                                }
                                else
                                {
                                    sets.push(next);
                                }
                            }
                            else if (chr === ']')
                            {
                                break;
                            }
                            else if (chr === '^')
                            {
                                var before = sets[ sets.length - 1];

                                if(!before)
                                {
                                    negative = true;
                                }
                                else
                                {
                                    sets.push(chr);
                                }
                            }
                            else if (chr === '-')
                            {
                                var next = tree.shift();

                                if (next === ']')
                                {
                                    sets.push(chr);
                                    chr = next;
                                    break;
                                }

                                var before = sets[ sets.length - 1];

                                if (!before)
                                {
                                    sets.push(chr);
                                }
                                else
                                {
                                    for (var i: number = before.charCodeAt(0) + 1, len: number = next.charCodeAt(0); i < len; i++)
                                    {
                                        sets.push(String.fromCharCode(i));
                                    }
                                }
                            }
                            else
                            {
                                sets.push(chr);
                            }
                        }
                        if(chr !== ']')
                        {
                            throw "missmatch bracket: " + chr;
                        }

                        if(negative)
                        {
                            var neg = {};

                            for(var i: number = 0, len: number = sets.length; i < len; i++)
                            {
                                neg[sets[i]] = true;
                            }

                            candinates = [];

                            for(var i: number = 0, len: number = ValueGenerator.ALL.length; i < len; i++)
                            {
                                if (!neg[ValueGenerator.ALL[i]]) candinates.push(ValueGenerator.ALL[i]);
                            }
                        }
                        else
                        {
                            candinates = sets;
                        }

                        break;
                    case '.':
                        ret += choice();
                        candinates = ValueGenerator.ALL;
                        break;
                    default:
                        ret += choice();
                        candinates = chr;
                }
            }

            return ret + choice();
        }

        /**
         *
         * @param tree
         * @returns {*[]}
         */
        private processSelect(tree): Array<any>
        {
            var candinates = [[]];

            while(tree.length)
            {
                var chr: string = tree.shift();

                if(chr === '\\')
                {
                    var next = tree.shift();

                    if(next === '|')
                    {
                        candinates[0].push(next);
                    }
                    else
                    {
                        candinates[0].push(chr, next);
                    }
                }
                else if (chr === '[')
                {
                    candinates[0].push(chr);

                    while (tree.length)
                    {
                        chr = tree.shift();
                        candinates[0].push(chr);

                        if(chr === '\\')
                        {
                            var next = tree.shift(); // no warnings
                            candinates[0].push(next);
                        }
                        else if (chr === ']')
                        {
                            break;
                        }
                    }
                }
                else if (chr === '|')
                {
                    candinates.unshift([]);
                }
                else
                {
                    candinates[0].push(chr);
                }
            }

            for(var i: number = 0, it; (it = candinates[i]); i++)
            {
                tree.push(it);

                for (var j: number = 0, len: number = it.length; j < len; j++)
                {
                    if (it[j] instanceof Array)
                    {
                        this.processSelect(it[j]);
                    }
                }
            }

            return [tree];
        }

        /**
         *
         * @param pattern
         * @returns {string}
         */
        public generateValue(pattern): string|number
        {
            if (pattern instanceof RegExp)
            {
                pattern = pattern.source;
            }

            this._SetCounter(1);

            var tree;

            tree = this.processGrouping(pattern.split(''));
            tree = this.processSelect(tree);
            return this.processOthers(tree);
        }

        /**
         *
         * @param num
         * @private
         */
        private _SetCounter(num: number): void
        {
            this._Counter = num;
        }
    }
}