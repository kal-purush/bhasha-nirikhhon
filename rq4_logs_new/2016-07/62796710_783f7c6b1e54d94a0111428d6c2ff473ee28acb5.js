var board = new Array();
var score = 0;
var hasConflicted = new Array();
var documentWidth=window.screen.availWidth;
var gridContainerWidth=0.92*documentWidth;
var cellSideLength=0.18*documentWidth;
var cellSpace=0.04*documentWidth;
var startX=0;
var startY=0;
var endX=0;
var endY=0;



$(document).ready(function(){
	prepareForMobile();
	newgame();
});

function prepareForMobile(){
	if(documentWidth>500){
		gridContainerWidth=500;
		cellSpace=20;
		cellSideLength=100;
	}
	$('#grid-container').css('width',gridContainerWidth-2*cellSpace+'px');
	$('#grid-container').css('height',gridContainerWidth-2*cellSpace+'px');
	$('#grid-container').css('padding',cellSpace+'px');
	$('#grid-container').css('border-radius',0.02*gridContainerWidth+'px');
	$('.grid-cell').css('width',cellSideLength+'px');
	$('.grid-cell').css('height',cellSideLength+'px');
	$('.grid-cell').css('border-radius',0.02*cellSideLength+'px');
}

function newgame(){
	init();
	generateOneNumber();
	generateOneNumber();
}

function init(){
	for(var i=0;i<4;i++){
		for(var j=0;j<4;j++){
			var gridCell = $('#grid-cell-'+i+'-'+j);
			gridCell.css('top',getPosTop(i,j));
			gridCell.css('left',getPosLeft(i,j));
		}
	}
	for(var i=0;i<4;i++){
		board[i] = new Array();
		hasConflicted[i]=new Array();
		for(var j=0;j<4;j++){
			board[i][j]=0;
			hasConflicted[i][j]=false;
		}
	}
	updateBoardView();
	score=0;
}

function updateBoardView(){
	$(".number-cell").remove();
	for(var i=0;i<4;i++){
		for(var j=0;j<4;j++){
			$('#grid-container').append('<div class="number-cell" id="number-cell-'+i+'-'+j+'"></div>');
			var theNumberCell = $('#number-cell-'+i+'-'+j);

			if(board[i][j]==0){
				theNumberCell.css('wdith','0px');
				theNumberCell.css('height','0px');
				theNumberCell.css('top',getPosTop(i,j)+cellSideLength/2+'px');
				theNumberCell.css('left',getPosLeft(i,j)+cellSideLength/2+'px');
			}else{
				theNumberCell.css('width', cellSideLength+'px');
				theNumberCell.css('height', cellSideLength+'px');
				theNumberCell.css('top',getPosTop(i,j)+'px');
				theNumberCell.css('left',getPosLeft(i,j)+'px');
				theNumberCell.css('background-color',getNumberBackgroundColor(board[i][j]));
				theNumberCell.css('color',getNumberColor(board[i][j]));
				theNumberCell.text(board[i][j]);
			}	
			hasConflicted[i][j]=false;
		}
	}
	$('.number-cell').css('line-height',cellSideLength+'px');
	$('.number-cell').css('font-size',0.6*cellSideLength+'px');
}

function generateOneNumber(){
	if(nospace(board)){
		return false;
	}
	var randxy=new Array();
	var randx=0;
	var randy=0;
	for(var i=0;i<4;i++){
		for(var j=0;j<4;j++){
			if(board[i][j]==0){
				randxy.push([i,j]);
			}
		}
	}
	var randomSelect=Math.floor(Math.random()*randxy.length);
	randx=randxy[randomSelect][0];
	randy=randxy[randomSelect][1];

	var randNumber=Math.random()<0.5?2:4;

	board[randx][randy]=randNumber;
	showNumberWithAnimation(randx,randy,randNumber);
	return true;
}
$(document).keydown(function(e){
	if(e.keyCode==37||e.keyCode==38||e.keyCode==39||e.keyCode==40){
		e.preventDefault();
	}
});
$(document).keyup(function(e){
	switch(e.keyCode){
		case 37:
			if(canMoveLeft(board)){
				if(moveLeft()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
			}
			break;
		case 38:
			if(canMoveUp(board)){
				if(moveUp()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
				
			}
			break;
		case 39:
			if(canMoveRight(board)){
				if(moveRight()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
				
			}
			break;
		case 40:
			if(canMoveDown(board)){
				if(moveDown()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
				
			}
			break;
	}
});

document.addEventListener('touchstart',function(e){
	startX=e.touches[0].pageX;
	startY=e.touches[0].pageY;
})
document.addEventListener('touchend',function(e){
	stopX=e.changedTouches[0].pageX;
	stopY=e.changedTouches[0].pageY;

	var deltaX=stopX-startX;
	var deltaY=stopY-startY;
	if(Math.abs(deltaX)>=Math.abs(deltaY)){
		if(deltaX>80){
			if(canMoveRight(board)){
				if(moveRight()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
				
			}
		}
		if(deltaX<-80){
			if(canMoveLeft(board)){
				if(moveLeft()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
			}
		}
	}else{
		if(deltaY>80){
			if(canMoveDown(board)){
				if(moveDown()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
				
			}
		}
		if(deltaY<-80){
			if(canMoveUp(board)){
				if(moveUp()){
					setTimeout(generateOneNumber(),210);
					setTimeout(isgameover(),300);
				}
				
			}
		}
	}
})

function isgameover(){
	if(nospace(board)&&nomove(board)){
		gameover();
	}
}
function gameover(){
	alert('gameover');	
}

function moveLeft(){
	for(var i=0;i<4;i++){
		for(var j=1;j<4;j++){
			if(board[i][j]!=0){
				for(var k=0;k<j;k++){
					if(board[i][k]==0&&noBlockHorizontal(i,k,j,board)){
						showMoveAnimation(i,j,i,k);
						board[i][k]=board[i][j];
						board[i][j]=0;
						continue;
					}else if(board[i][k]==board[i][j]&&noBlockHorizontal(i,k,j,board)&&!hasConflicted[i][k]){
						showMoveAnimation(i,j,i,k);
						board[i][k]+=board[i][j];
						board[i][j]=0;
						score+=board[i][k];
						updatescore(score);
						hasConflicted[i][k]=true;
						continue;
					}
				}
			}
		}
	}
	setTimeout('updateBoardView()',200);
	return true;
}
function moveRight(){
	for(var i=0;i<4;i++){
		for(var j=2;j>-1;j--){
			if(board[i][j]!=0){
				for(var k=3;k>j;k--){
					if(board[i][k]==0&&noBlockHorizontal(i,k,j,board)){
						showMoveAnimation(i,j,i,k);
						board[i][k]=board[i][j];
						board[i][j]=0;
						continue;
					}else if(board[i][k]==board[i][j]&&noBlockHorizontal(i,j,k,board)&&!hasConflicted[i][k]){
						showMoveAnimation(i,j,i,k);
						board[i][k]+=board[i][j];
						board[i][j]=0;
						score+=board[i][k];
						updatescore(score);
						hasConflicted[i][k]=true;
						continue;
					}
				}
			}
		}
	}
	setTimeout('updateBoardView()',200);
	return true;
}
function moveUp(){
	for(var j=0;j<4;j++){
		for(var i=1;i<4;i++){
			if(board[i][j]!=0){
				for(var k=0;k<i;k++){
					if(board[k][j]==0&&noBlockVertical(k,i,j,board)){
						showMoveAnimation(i,j,k,j);
						board[k][j]=board[i][j];
						board[i][j]=0;
						continue;
					}else if(board[k][j]==board[i][j]&&noBlockVertical(k,i,j,board)&&!hasConflicted[k][j]){
						showMoveAnimation(i,j,k,j);
						board[k][j]+=board[i][j];
						board[i][j]=0;
						score+=board[k][j];
						updatescore(score);
						hasConflicted[k][j]=true;
						continue;
					}
				}
			}
		}
	}
	setTimeout('updateBoardView()',200);
	return true;
}
function moveDown(){
	for(var j=0;j<4;j++){
		for(var i=2;i>-1;i--){
			if(board[i][j]!=0){
				for(var k=3;k>i;k--){
					if(board[k][j]==0&&noBlockVertical(k,i,j,board)){
						showMoveAnimation(i,j,k,j);
						board[k][j]=board[i][j];
						board[i][j]=0;
						continue;
					}else if(board[k][j]==board[i][j]&&noBlockVertical(i,k,j,board)&&!hasConflicted[k][j]){
						showMoveAnimation(i,j,k,j);
						board[k][j]+=board[i][j];
						board[i][j]=0;
						score+=board[k][j];
						updatescore(score);
						hasConflicted[k][j]=true;
						continue;
					}
				}
			}
		}
	}
	setTimeout('updateBoardView()',200);
	return true;
}