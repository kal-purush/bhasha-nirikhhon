var budgetAmount = document.querySelector("#budgetAmount");
var expenseAmount = document.querySelector("#expenseAmount");
var dollarAmount = document.querySelector("#dollarAmount");
var creditAmount = document.querySelector("#creditAmount");
var balanceAmount = document.querySelector("#balanceAmount");
var bSubmit = document.querySelector("#bSubmit");
var eSubmit = document.querySelector("#eSubmit");
var balance = 0;

bSubmit.addEventListener("click", function(event){
	dollarAmount.textContent = "$" + Number(budgetAmount.value); 
	balance += Number(budgetAmount.value);
	balanceAmount.textContent = "$" + balance;
	color();
	event.preventDefault();
});

eSubmit.addEventListener("click", function(event){
	creditAmount.textContent = "$" + Number(expenseAmount.value); 
	balance -= Number(expenseAmount.value);
	balanceAmount.textContent = "$" + balance;
	color();
	event.preventDefault();
});

function color(){
	if(balance === 0){
	balanceAmount.style.color = "black";
	} else if(balance > 0){
		balanceAmount.style.color = "green";
	} else if(balance < 0){
		balanceAmount.style.color = "red";
	}
}


$("#eSubmit").on("click", function(){
	var expense = $("#expenseName").val();
	var amount = $("#expenseAmount").val();
	$("#expenseName").val("");
	$("#expenseAmount").val("");
	$('tbody').append('<tr><td>' + expense + '</td><td> $' + amount + '</td></tr>')
	return false;
});