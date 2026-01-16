/*

Course: INFO2180 Project 2
Title: 15 Puzzle Assignment 
Student: Ajani Bissick

*/

var eSpaceX = '300px';
var eSpaceY = '300px';

window.onload = function(){
    var puzzleArea = document.getElementById('puzzlearea');
    tiles = puzzleArea.getElementsByTagName('div');
  
    for (var i=0; i<tiles.length; i++)                              
    { 
      //PuzzleArea initialization : background image is setup beneath the fifteen tiles 
        tiles[i].style.backgroundImage="url('eeyore.jpg')";  
        tiles[i].className = 'puzzlepiece';                         
        tiles[i].style.left = (i%4*100)+'px';
        tiles[i].style.top = (parseInt(i/4)*100) + 'px';
        tiles[i].style.backgroundPosition= '-' + tiles[i].style.left + ' ' + '-' + tiles[i].style.top;
		
    }
}