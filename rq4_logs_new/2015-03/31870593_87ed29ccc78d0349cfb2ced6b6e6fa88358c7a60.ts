/* Functions for calculating dart games */

module darts {
    
    export function parse_score(score: string) {
        var parts = score.split(/\s+/);
        var re = /([dDtT])?(\d{1,2}|[bB])/;
        var total: number = 0;
        for (var i in parts) {
            var m = re.exec(parts[i]);
            var value: number = 0;
            if (m){
            if (m[2]) {
                value = parseInt(m[2]);
                if (!value) value = 25;
                switch (m[1]) {
                    case 't':
                    case 'T':
                        value *= 3;
                        break;
                    case 'd':
                    case 'D':
                        value *=2;
                }
            }
            total += value;
            }
        }
        return total;
    }
}