

var $checkBalance = $('#checking-balance');
	$checkBalance = $checkBalance.html();
	$checkBalance = parseInt($checkBalance.slice(1));

	var $savingsBalance = $('#savings-balance');
	$savingsBalance = $savingsBalance.html();
	$savingsBalance = parseInt($savingsBalance.slice(1));
	

// depositing money in the checking acount.
var checkDepClick = function(){
	$checkBalance = $checkBalance + parseInt($('#checking-amount').val());
	$('#checking-balance').html("$" + $checkBalance);
	$('#checking-balance').css("background-color", "#E3E3E3");

}

$('#checking-deposit').click(checkDepClick);

// withdrawing money from checking acount.
var checkWithClick = function(){
	if ($checkBalance >= $('#checking-amount').val() ){
		$checkBalance = $checkBalance - parseInt($('#checking-amount').val());
		$('#checking-balance').html("$" + $checkBalance);
	} else if ($checkBalance + $savingsBalance >= $('#checking-amount').val()){
		$savingsBalance = $savingsBalance - ($('#checking-amount').val() - $checkBalance);
		$checkBalance = $checkBalance - $checkBalance;
		$('#checking-balance').html("$" + $checkBalance);
		$('#savings-balance').html("$" + $savingsBalance);
	}
	if ($checkBalance === 0){
		$('#checking-balance').css("background-color", "red");
	} 	
}

$('#checking-withdraw').click(checkWithClick);



// depositing money in the savings acount.
var savingsDepClick = function(){
	$savingsBalance = $savingsBalance + parseInt($('#savings-amount').val());
	$('#savings-balance').html("$" + $savingsBalance);
	$('#savings-balance').css("background-color", "#E3E3E3");

} 

$('#savings-deposit').click(savingsDepClick);



// withdrawing money from savings acount.
var savingsWithClick = function(){
	if ($savingsBalance >= $('#savings-amount').val() ) {
		$savingsBalance = $savingsBalance - parseInt($('#savings-amount').val());
		$('#savings-balance').html("$" + $savingsBalance);
	} else if ($savingsBalance + $checkBalance >= $('#savings-amount').val() ) {
		$checkBalance = $checkBalance - ($('#savings-amount').val() - $savingsBalance);
		$savingsBalance = $savingsBalance - $savingsBalance;
		$('#checking-balance').html("$" + $checkBalance);
		$('#savings-balance').html("$" + $savingsBalance);
	}
	if ($savingsBalance === 0){
		$('#savings-balance').css("background-color", "red");
	} 
}

$('#savings-withdraw').click(savingsWithClick);

