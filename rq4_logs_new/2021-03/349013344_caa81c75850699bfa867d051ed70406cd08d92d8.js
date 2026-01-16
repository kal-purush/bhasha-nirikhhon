// let a = ['2','/','(','2','+','3','+','4',')'];
// // let a =['2','+','3','x','4','+','5'];
// // let a = ['5','+','(','(','1','+','2',')','x','4',')','+','3'];
// let operator = [];
// let operand = [];

// work like stack
function travesal(operator,operand){
		var j = operator.length-1;// opertor _ j is ')'
		// console.log(" length : "+operator);
		while(operator[j]!='('){
			operand.push(operator[j]);
			
			operator = operator.slice(0,-1);
			
			j = j-1;
		}
		// console.log("here twice" + operator);
		operator = operator.slice(0,-1);
		// console.log("here twice hehe" + operator);
		return [operator,operand];
		

}

export function convert_to_postfix(arr){
	
	// console.log(operator);
	let operator = [];
	let operand = [];
	console.log(" here you are: "+ arr);
	for(var i = 0; i<arr.length; i++){
		// case 1: if a_i is number 
		if(i==0 && "+-x/".includes(arr[i])){
			console.log("type error");
			return "type error";
		}

		else if(("+-x/".includes(arr[i-1]) && "+-x/".includes(arr[i])) && i>0){
			console.log("type error");
			return "type error";

		}
		else{
			// if the eqquation dont have any error
			
			if(!'+-x/()'.includes(arr[i])){
				operand.push(parseFloat(arr[i]));
				if(operator.length>0 &&'x/'.includes(operator[operator.length-1])){
					// if a_i is number and last elemnt of ooperator is x/ => push last elemnet of operator to operand
					operand.push(operator[operator.length-1]);
					operator = operator.slice(0,-1);
				}else {
					// if(operator.length>=2 &&)
				}
			}
		// case 2: a_i is x/+-
			else{
				if('+-'.includes(arr[i]) && '+-'.includes(operator[operator.length-1])){
					operand.push(operator[operator.length-1]);
					operator = operator.slice(0,-1);
				}
				
				
				if(arr[i]==')'){
					let temp = travesal(operator,operand);
					console.log('here : '+temp[0]);
					console.log('here 2: '+temp[1]);
					operator =  temp[0];
					operand = temp[1];
					

				}else{
					operator.push(arr[i]);
				}

			}
			if(i==arr.length-1 && operator.length==1){
				operand.push(operator[operator.length-1]);
				operator = [];
			}
			// console.log(" i = "+i);
			// console.log(operand);
			// console.log(operator);
		}

	}
	return operand;

 }

function div_mul_add_sub(operand,i){
	let tmp ;
	switch(true){
		case operand[i]=='+':
			  tmp = operand[i-1]+operand[i-2];
			  break;
		case operand[i]=='-':
			  tmp= operand[i-2]-operand[i-1];
			  break;
		case operand[i]=='x':
			  tmp = operand[i-2]*operand[i-1];
			  break;
		case operand[i]=='/':
			 tmp = operand[i-2]/operand[i-1];
			 break;
		
	}
	operand[i-2] = tmp;
	operand.splice(i-1,2);
	return operand;
}
export function reverse_polish(operand){
	// traversal nguoc mangr operand
	// let result = []; // first in , first out, queue
	if(operand==="type error"){
		return "type error";
	}
	var L = operand.length-1;
	let i = L;
	// console.log(operand);
	if (operand.length == 1){
		return operand;
	}
	while(i>=0){
		console.log("i = "+i);
		if('+-x/'.includes(operand[i])){

			if(!'+-x/'.includes(operand[i-1]) && !'+-x/'.includes(operand[i-2])){
				operand = div_mul_add_sub(operand, i);
				// result.push(temp);
				i = i-3;

			}else{
				// result.push(operand[i]);
				i = i-1;

			}
		}else{
			// result.push(operand[i]);
			i = i-1;
		}
		
		console.log(operand);
	}
	// operand = result;
	operand  = reverse_polish(operand);
	return operand;

	
	
}
//  convert_to_postfix();
//  let b = reverse_polish(operand);


//  export{convert_to_postfix};