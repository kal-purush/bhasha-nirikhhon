// Practice Problem 30
// Randomize numbers based on start and end values from parameter. If start number is an array, return a random value from the array.

function random_range(start_num,end_num){
    if(Array.isArray(start_num)){
        return start_num[Math.floor(Math.random()*(start_num.length))];
    } else {
        return Math.floor(Math.random()*(end_num - start_num + 1)+start_num);
    }
}