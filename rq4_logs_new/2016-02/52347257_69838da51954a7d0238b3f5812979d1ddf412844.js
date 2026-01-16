function umn() {
	var a  = prompt('Введите первое число');
	var b = prompt('Введите второе число');
	if(isNaN(a)==false & isNaN(b)==false) { 
			alert('Произведение введенных чисел ='+a*b);
	}else{
		alert('Введены не числа!');
	}
}

function slow() { 
	var a  = prompt('Введите первое число');
	var b = prompt('Введите второе число');
	var c = parseInt(a)+parseInt(b);
	if(isNaN(a)==false & isNaN(b)==false) { 
			alert('Сумма введенных чисел = '+c);
	}else{
		alert('Введены не числа!');
	}
}

function del() {
	var a  = prompt('Введите первое число');
	var b = prompt('Введите второе число');
	if(isNaN(a)==false & isNaN(b)==false) { 
			alert('Частное введенных чисел = '+a/b);
	}else{
		alert('Введены не числа!');
	}
}


function vith(){
	var a  = prompt('Введите первое число');
	var b = prompt('Введите второе число');
	var c = parseInt(a) - parseInt(b);
	if(isNaN(a)==false & isNaN(b)==false) { 
			alert('Разность введенных чисел = '+ c);
	}else{
		alert('Введены не числа!');
	}


}
 		console.log('by kott9ra');