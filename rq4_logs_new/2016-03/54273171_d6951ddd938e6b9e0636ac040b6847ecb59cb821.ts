class GameTimer
{

  public currentTimer:number;
  public totalTimer:number;
  public stage:createjs.Stage;
  public gameContainer:createjs.MovieClip;
  public timerImage:createjs.Bitmap;
  public imageUrl:string = "asset/card/cardClubs1.png";
  private initScale:number = 2;

  public GameTimer = function()
  {

  }

  public init(stage:createjs.Stage,gameContainer:createjs.MovieClip):void
  {
    this.stage = stage;
    this.gameContainer = gameContainer;
    this.timerImage = new createjs.Bitmap(this.imageUrl);
    this.gameContainer.addChild(this.timerImage);
    this.timerImage.scaleX = this.initScale;
    this.timerImage.scaleY = 0.01;
  }

  public update(scaleFactor:number):void
  {
      this.timerImage.scaleX = scaleFactor*this.initScale;
  }

}