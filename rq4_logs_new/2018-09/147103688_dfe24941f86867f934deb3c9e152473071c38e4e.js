var gv_MyGame = {};
var gv_LevelComplete = 0;
var gv_LevelDifficulty = 0;
var gv_StandartGameWidth = 800;
var gv_StandartGameHeight = 600;

window.onload = function() {

	m_FirstBootGame();

};
window.addEventListener("resize", function() {
    
    m_DefineGameSizeAndScale();
	gv_MyGame.game.scale.setGameSize(gv_MyGame.GameWidth, gv_MyGame.GameHeight);

}, false);
//-------------------------------------------------------------------------------------------------------------------
function m_FirstBootGame()
{
	// gv_MyGame.GameWidth = 1000;
	// gv_MyGame.GameHeight = 800;
	
    m_DefineGameSizeAndScale();
	
	gv_MyGame.game = new Phaser.Game(gv_MyGame.GameWidth, gv_MyGame.GameHeight, Phaser.CANVAS, '');//Phaser.AUTO
	//var lv_game = new Phaser.Game(640, 480, Phaser.AUTO, '');

	//Phaser.game.scale.scaleMode = Phaser.ScaleManager.SHOW_ALL;
	// lv_game.scale.pageAlignHorizontally = true;
	// lv_game.scale.pageAlignVertically = true;
	
	//gv_MyGame.game = lv_game;

	gv_MyGame.game.state.add("mavOS", gv_MyGame.mavOS);
    //gv_MyGame.game.state.add("ZackPuzzleGame", gv_MyGame.ZackPuzzleGame);
	//gv_MyGame.game.state.add("CodyPuzzleGame", gv_MyGame.CodyPuzzleGame);

	gv_MyGame.game.state.start("mavOS");

}
//-------------------------------------------------------------------------------------------------------------------
function m_DefineGameSizeAndScale()
{
	/*
	gv_MyGame.GameWidth = window.innerWidth;
	gv_MyGame.GameHeight = window.innerHeight;
	gv_scaleRatio = window.devicePixelRatio / 3;

	var gv_scaleRatio_1 = gv_MyGame.GameWidth/gv_StandartGameWidth;// 700 - ������ �� ���������
	var gv_scaleRatio_2 = gv_MyGame.GameHeight/gv_StandartGameHeight;// 500 - ������ �� ���������
	
	if(gv_scaleRatio_2 < gv_scaleRatio_1)
	  gv_scaleRatio = gv_scaleRatio_2;
	else
	  gv_scaleRatio = gv_scaleRatio_1;
	*/
	
	gv_MyGame.GameWidth = gv_StandartGameWidth;
	gv_MyGame.GameHeight = gv_StandartGameHeight;
	gv_scaleRatio = 1;
	  
}








