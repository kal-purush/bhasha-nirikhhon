function bubbleSort(arr) {
    for(let i = 0; i < arr.length; i++){
        let swapped = false;
        for(let j = 0; j < arr.length-i; j++){
            if(arr[j] > arr[j+1]){
                let temp = arr[j];
                arr[j] = arr[j+1];
                arr[j+1] = temp;
                swapped = true;
            }

        }
        if(!swapped) break;
    }
    return arr;
}