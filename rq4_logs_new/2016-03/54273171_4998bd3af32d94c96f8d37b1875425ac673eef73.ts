class MainMenu
{
  public mainImage:createjs.Bitmap;
  public mainButton:createjs.MovieClip;
  private stage:createjs.Stage;
  public MainMenu = function()
  {

  }
  public callMainMenu(stage:createjs.Stage):void
  {
    this.stage = stage;
    this.mainImage = new createjs.Bitmap("asset/card/cardBack_blue1.png");
    this.mainButton = new createjs.MovieClip();
    this.mainButton.addChild(this.mainImage);
    this.stage.addChild(this.mainButton);
  }

  public destroyThis():void
  {
    this.mainButton.removeAllEventListeners("click");
    this.mainButton.removeChild(this.mainImage);
    this.stage.removeChild(this.mainButton);
  }
}