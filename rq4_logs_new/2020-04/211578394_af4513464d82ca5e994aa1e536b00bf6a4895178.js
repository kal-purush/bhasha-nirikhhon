/**
 * @param {string} s
 * @param {string} p
 * @return {boolean}
 */
const isMatch = function (s, p) {
    let dp = new Array(s.length + 1);
    for (let i = 0; i < s.length + 1; i++) {
        dp[i] = new Array(p.length + 1);
        for (let j = 0; j < p.length + 1; j++) {
            dp[i][j] = false;
        }
    }
    dp[0][0] = true;
    match(s, p, dp);
    return dp[s.length][p.length];
};
function compareChar(a, reg) {
    if (a === reg || reg === '.')
        return true;
    else
        return false;
}
function findAtomReg(s, i) {
    if (s[i + 1] === '*') {
        return s[i] + '*';
    }
    else {
        return s[i];
    }
}
function matchUntil(s, from, reg) {
    if (reg.length > 1) {
        let i = 0;
        while (s[from + i] && compareChar(s[from + i], reg[0])) {
            i += 1;
        }
        return i;
    }
    else {
        if (compareChar(s[from], reg)) {
            return 1;
        }
        else {
            throw new Error('unmatch');
        }
    }
}
function match(s, p, dp) {
    let _s = ' ' + s;
    let _p = ' ' + p;
    let atomReg;
    let j = 0;
    while (j + 1 < _p.length) {
        atomReg = findAtomReg(_p, j + 1);
        for (let i = 0; i < _s.length; i++) {
            if (dp[i][j]) {
                try {
                    let matchTo = matchUntil(_s, i + 1, atomReg);
                    if (atomReg.length === 2) {
                        for (let tmp = 0; tmp <= matchTo; tmp++) {
                            dp[i + tmp][j + 2] = true;
                        }
                    }
                    else {
                        if (matchTo > 0) {
                            dp[i + 1][j + 1] = true;
                        }
                    }
                }
                catch (e) {
                    continue;
                }
            }
        }
        j += atomReg.length;
    }
}
console.log(isMatch('aaa', 'aaaa'));
//# sourceMappingURL=solution10.js.map