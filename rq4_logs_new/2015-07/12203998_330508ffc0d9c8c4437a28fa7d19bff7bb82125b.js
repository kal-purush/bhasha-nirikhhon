/* stuff this function reminded me of was that JavaScripts sort fxn lexicographically sorts so you have to pass in a custom compare fxn to properly sort 
   numbers. One other thing is that if you iterate through an array and delete in the middle of the loop, iterate backwards so you aren't affected by
   the length change
*/
function processData(input) {
    //Enter your code here
    input = '8\n1 13 3 8 14 9 4 4';
    input = input.split('\n');
    var matches = input.shift(),
        lengths = input.shift().split(' ')
                               .map(function(x) { return parseInt(x, 10); })
                               .sort(function(a, b) { return b - a; });

    while (true) {
        var min = lengths[matches - 1],
            cuts = 0;
        
        for (var i = lengths.length - 1; i >= 0; --i) {
            lengths[i] -= min;
            ++cuts;
            
            if (lengths[i] === 0) {
                lengths.splice(i, 1);
            }
        }
        console.log(cuts);

        if (lengths.length === 0) {
            break;
        }
        
        matches = lengths.length;
    }
} 