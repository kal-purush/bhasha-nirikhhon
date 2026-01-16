function getLevel(url,credit){
	var imghtml = "";
	if(fanwei(1,500,credit)){
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
	}
	if(fanwei(500,5000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
	}
	if(fanwei(5000,20000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
	}
	if(fanwei(20000,50000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
	}
	if(fanwei(50000,100000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level1.png"+"'/>";
	}
	if(fanwei(100000,300000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
	}
	if(fanwei(300000,1000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
	}
	if(fanwei(1000000,2000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
	}
	if(fanwei(2000000,5000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
	}
	if(fanwei(5000000,10000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level2.png"+"'/>";
	}
	if(fanwei(10000000,20000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
	}
	if(fanwei(20000000,50000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
	}
	if(fanwei(50000000,200000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
	}
	if(fanwei(200000000,500000000,credit)){
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
	}
	if(credit>=500000000){
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
		imghtml += "<img src='"+url+"font/imgs/level3.png"+"'/>";
	}
	return imghtml;
}

function fanwei(num1,num2,credit){
	if(num1<=credit && credit <num2)
		return true;
	return false;
}