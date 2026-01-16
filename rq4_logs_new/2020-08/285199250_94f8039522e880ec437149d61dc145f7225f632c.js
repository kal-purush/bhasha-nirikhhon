
//first question 
/*Clean the room function: given an input of [1,2,4,591,392,391,2,5,10,2,1,1,1,20,20], 
make a function that organizes these into individual array that is ordered. 
For example answer(ArrayFromAbove) should return: [[1,1,1,1],[2,2,2], 4,5,10,[20,20], 391, 392,591]. */


const cleanTheRoom = (arr) =>{
const answerArray = arr.sort((a,b) => a-b);
let result =[];
let resultArray =[];
let count = 0;
for(let i=0; i < answerArray.length;i++){
	 if(answerArray[i] === answerArray[i+1] || answerArray[i] === answerArray[i-1]){
	 	if(answerArray[i] !== answerArray[i+1]){
	 		count = 0;
	 		result.push(answerArray[i]);
	 		resultArray.push([result]);
	 		result = [];
	 	}
	 	else{
	 		result.push(answerArray[i]);
	 		count ++;
	 	}	 	
	 }	
	else{
		count = 0;
		resultArray.push(answerArray[i]);
	}
}
console.log("resultArray",resultArray);
}

cleanTheRoom([1,2,4,591,392,391,2,5,10,2,1,1,1,20,20,57,57,57,937402,849]); 

//BONUS QUESTION
//Make it so it organizes strings differently from number types. i.e. [1, "2", "3", 2] should return [[1,2], ["2", "3"]]

const sortStrings = (arr) => {
	const sortedArray = arr.sort((a,b) => a-b);
	const stringArray = [];
	const integerArray =[];
	let distinguishedArray =[];
	console.log("sortedArray",sortedArray);
	sortedArray.forEach((item,index) => {
		console.log(item,index);
		if(typeof(item) === "string"){
			stringArray.push(item);
		}
		else{
			integerArray.push(item);
		}
	})
	distinguishedArray = distinguishedArray.concat([stringArray],[integerArray]);
	console.log("distinguishedArray",distinguishedArray);

}


sortStrings([1,2,4,"591","392",391,2,5,10,2,1,1,1,20,20,57,57,57,"937402",849]);