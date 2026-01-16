///<reference path="../tsD/createjs.d.ts"/>

class MainGame
{

	private arrCard:Array<Object> = new Array();
	private card1:createjs.Bitmap;
	private card2:createjs.Bitmap;
	private preload:Object;
	private backUrl:string = "asset/card/cardBack_blue2.png";
	private width:number = 5;
	private height:number = 4;
	private margin:number = 5;
	private allContainer:createjs.MovieClip;

	private stage = new createjs.Stage("demoCanvas");

	public init()
	{
		this.generateCard();
	}

	private generateCard()
	{
		this.allContainer = new createjs.MovieClip();
		for(var i=0;i<this.width;i++)
		{
			for(var j=0;j<this.height;j++)
			{
        var c:Card = new Card();
				c.backImage = this.backUrl;
        c.init(this.stage,this.allContainer,i,j,this.margin);
			}
		}

		this.stage.addChild(this.allContainer);
		this.stage.update();
	}


	private updateStage = function():void
	{
		console.log("stage updated");
		this.stage.update();

	}



}