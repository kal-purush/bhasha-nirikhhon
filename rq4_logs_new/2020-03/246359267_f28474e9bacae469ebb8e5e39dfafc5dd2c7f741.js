class Calculator
{
	constructor(previousOperandTextElement,currentOperandTextElement)
	{
		this.previousOperandTextElement=previousOperandTextElement;
		this.currentOperandTextElement=currentOperandTextElement;
		this.allClear();
	}
 getDisplayNumber(number)
{
	const stringNumber=number.toString();
	const integerDigits=parseFloat(stringNumber.split('.')[0]);
	const decimalDigits=stringNumber.split('.')[1];
	let integerDisplay;
	if(isNaN(integerDigits))
	{
		integerDisplay='';
	}
	else
	{
		integerDisplay=integerDigits.toLocaleString('en',{maximumFractionDigits:0});
	}
	if (decimalDigits!=null) {
		return `${integerDigits}.${decimalDigits}`;
	}
	else
	{
		return integerDisplay;
	}
}
 updateDisplay()
{
	this.currentOperandTextElement.innerText=this.getDisplayNumber(this.currentOperand);
	if(this.operation!=null)
	{
		this.previousOperandTextElement.innerText=`${this.getDisplayNumber(this.previousOperand)}${this.operation}`;
	}
	else
	{
		this.previousOperandTextElement.innerText='';
	}
}
 appendNumber(number)
{
	if(number==='.'&&this.currentOperand.includes('.')) return;
	this.currentOperand=this.currentOperand.toString()+number.toString();
}
deleteNumber()
{
	this.currentOperand=this.currentOperand.toString().slice(0,-1);
}
 allClear()
{
	this.previousOperand="";
	this.currentOperand="";
	this.operation=undefined;
}
 chooseOperation(operation)
{
	if(this.currentOperand==="") return;
	if(this.currentOperand!=="")
		this.compute;
	this.operation=operation;
	this.previousOperand=this.currentOperand;
	this.currentOperand='';
}
 compute()
{
	let computation;
	const prev=parseFloat(this.previousOperand);
	const cur=parseFloat(this.currentOperand);
	if(isNaN(prev)||isNaN(cur)) return;
	switch(this.operation)
	{
		case '+':
		computation=prev+cur;
		break;
		case '-':
		computation=prev-cur;
		break;
		case '*':
		computation=prev*cur;
		break;
		case '÷':
		computation=prev/cur;
		break;
		case '%':
		computation=prev%cur;
		break;
	}	
	this.currentOperand=computation;
	this.operation=undefined;
	this.previousOperand='';
}

}

const numberButtons=document.querySelectorAll('[data-number]');
const operationButtons=document.querySelectorAll('[data-operation]');
const equalButton=document.querySelector('[data-equals]');
const deleteButton=document.querySelector('[data-delete]');
const allClearButton=document.querySelector('[data-all-clear]');
const previousOperandTextElement=document.querySelector('[data-previous-operand]');
const currentOperandTextElement=document.querySelector('[data-current-operand]');

const calculator=new Calculator(previousOperandTextElement,currentOperandTextElement);

numberButtons.forEach(button=>{
	button.addEventListener('click',()=>{
		calculator.appendNumber(button.innerText)
		calculator.updateDisplay();
	})
})
operationButtons.forEach(button=>{
	button.addEventListener('click',()=>{
		calculator.chooseOperation(button.innerText)
		calculator.updateDisplay();
	})
})
equalButton.addEventListener('click',button=>{
	calculator.compute();
	calculator.updateDisplay();
})
allClearButton.addEventListener('click',button=>{
	calculator.allClear();
	calculator.updateDisplay();
})
deleteButton.addEventListener('click',button=>{
	calculator.deleteNumber();
	calculator.updateDisplay();
})












