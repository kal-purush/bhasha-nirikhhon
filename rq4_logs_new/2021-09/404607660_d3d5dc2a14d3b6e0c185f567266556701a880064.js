function getRandomInt(min = 0, max = 100)
{
	min = Math.ceil(min);
	max = Math.floor(max);
	return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
}

function genPassword(len = 16, usespecial = false, usealphaupp = true, usealphalow = true, usenumeric = true)
{
	let password_chars = [];
	let password = "";

	const alphalow = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"];
	const alphaupp = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"];
	const numeric = ["0","1","2","3","4","5","6","7","8","9"];
	const special = [",",".",":",";","-","_","#","=","+","*","~","!","?","@","/","|","\\","$","%","&","<",">","(",")","{","}","[","]","^","'","`","\""];

	if(usealphalow)
		password_chars = password_chars.concat(alphalow);

	if(usealphaupp)
		password_chars = password_chars.concat(alphaupp);

	if(usenumeric)
		password_chars = password_chars.concat(numeric);

	if(usespecial)
		password_chars = password_chars.concat(special);

	const charslen = password_chars.length;

	for(i=0; i<len; i++)
	{
		password = password + password_chars[getRandomInt(0,charslen)];
	}

	return password;
}