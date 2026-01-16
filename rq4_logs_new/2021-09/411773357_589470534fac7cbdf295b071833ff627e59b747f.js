function pickPeaks(arr) {
    var pos = [],
        peaks = [];
    for (let i = 1; i < arr.length - 1; i++) {
        if (arr[i - 1] < arr[i] && arr[i] >= arr[i + 1]) {
           
            peaks.push(arr[i]);
            pos.push(i);
        } else if (arr[i - 1] === arr[i] && arr[i] === arr[i + 1]) {
            peaks.push(arr[i-1]);
            pos.push(i-1);
        }
    }
    return {
        pos,
        peaks
    }
}
console.log(pickPeaks([1,2,5,4,3,2,3,6,4,1,2,3,3,4,5,3,2,1,2,3,5,5,4,3]));