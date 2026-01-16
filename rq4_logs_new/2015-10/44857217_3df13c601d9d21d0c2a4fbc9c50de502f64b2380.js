var str = prompt('Enter text');
window.onload = function(){
	var translate = (function(str){
		var ru = 'йцукенгшщзхъфывапролджэячсмитьбю.',
			en =   'qwertyuiop[]asdfghjkl;\'zxcvbnm,./',
			answ = '';
		for(var i = 0; i < str.length; i++){
			var k = 0;
			for(var j = 0; j < en.length; j++){
				if(str[i] == en[j]){
					answ += ru[j];
					k++;
					break;
				}
			}
			if(!k){
				answ += str[i];
			}
		}
		document.getElementsByTagName('div')[0].innerHTML = '<h2>' + answ + '</h2>';
	})(str);
}