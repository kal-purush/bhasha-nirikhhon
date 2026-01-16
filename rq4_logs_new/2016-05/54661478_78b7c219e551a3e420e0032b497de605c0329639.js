$(document).ready(function() {	
	
	$("#emptyCode").hide();
	$("#wrongCode").hide();
	
	var blankInput = false;
	var correctCaptcha = true;
	var validInput  = true;
	
	
	$("#btnUpload").click(function() {
		blankInput = checkBlankInput();
		
		if (blankInput == false && correctCaptcha == true && validInput == true) {
			//submit form
		}
		return false;
	})
})

function checkBlankInput() {
	var isBlank = false;	
	var $input = $("input:text, select").not("input[id='txtAddressLine2']");
	var $select = $("select");
	var $formGroup;
	var $this;
	var $parentDiv;
	
	$input.each(function(index, element){
		$this = $(element);
		$formGroup = $this.closest("div[class=form-group]");		
		$parentDiv = $this.parent();
		if($this.val() == "" || $this.val() == "-Select-") {
			$formGroup.addClass("has-error has-feedback");
			$parentDiv.append('<span class="glyphicon glyphicon-remove form-control-feedback"></span>');
			$parentDiv.append('<span class="help-inline text-danger">This field is required</span>');
			isBlank = true;
		} else {
			$formGroup.removeClass("has-error").addClass("has-success");
			$this.next("span[id^=errorIcon]").remove();
			$this.next("span[class=help-inline]").remove();
			isBlank = false;
		}
	})
	
	return isBlank;
}

function checkPhone() {
	
}