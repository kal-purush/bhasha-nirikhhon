var bubbleSort = function(arr) {
	var bool = true;
	while(bool){
		bool = false;
		for(let i = 0; i < arr.length-1; i++){
			let first = arr[i];
			let next = arr[i+1];
			if(arr[i] > next){
				arr[i+1] = first;
				arr[i] = next;
				bool = true;
			}			
		}
	}
	return arr;
};