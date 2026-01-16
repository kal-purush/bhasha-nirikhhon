class UI
{

  public mainImage:createjs.Bitmap;
  public logoImage:createjs.Bitmap;
  public logo2Image:createjs.Bitmap;
  public pausePanel:createjs.Bitmap;
  private pauseImage:createjs.Bitmap;
  private continueImage:createjs.Bitmap;
  private homeImage:createjs.Bitmap;

  private whiteBorder:createjs.Bitmap;


  public mainButton:createjs.MovieClip;
  public pauseButton:createjs.MovieClip;
  public continueButton:createjs.MovieClip;
  public homeButton:createjs.MovieClip;

  private stage:createjs.Stage;
  private logoUrl:string;
  private mainGame:MainGame;


  constructor(mainGame:MainGame)
  {
    this.mainGame = mainGame;
  }

  public callMainMenu(stage:createjs.Stage):void
  {

    this.stage = stage;
    this.mainImage = new createjs.Bitmap(PreloadGame.queue.getResult("main-button"));
    this.mainButton = new createjs.MovieClip();
    this.mainButton.addChild(this.mainImage);
    this.mainButton.scaleX = MainGame.GameWidth/6/this.mainImage.image.width;
    this.mainButton.scaleY = this.mainButton.scaleX;
    this.stage.addChild(this.mainButton);

    //create logo
    this.logoImage = new createjs.Bitmap(PreloadGame.queue.getResult("corner-logo"));
    this.logoImage.scaleX = MainGame.GameWidth/10/this.logoImage.image.width;
    this.logoImage.scaleY = this.logoImage.scaleX;

    this.stage.addChild(this.logoImage);

    this.logo2Image = new createjs.Bitmap(PreloadGame.queue.getResult("title-image"));
    this.logo2Image.scaleY = (MainGame.GameHeight-(MainGame.GameHeight/12))/this.logo2Image.image.width;
    this.logo2Image.scaleX =this.logo2Image.scaleY;

    this.stage.addChild(this.logo2Image);

    this.reposisi();
    //craeteListener
    this.mainButton.addEventListener("click",()=>this.startGame());
  }


  public CallGameUi()
  {
    this.pauseImage = new createjs.Bitmap(PreloadGame.queue.getResult("pause"));
    this.pauseImage.scaleX = MainGame.GameWidth/25/this.pauseImage.image.width;
    this.pauseImage.scaleY = this.pauseImage.scaleX;
    this.pauseImage.x = MainGame.GameWidth - (this.pauseImage.image.width * this.pauseImage.scaleX) - (MainGame.GameWidth/25);
    this.pauseImage.y = MainGame.GameHeight/25;
    this.pauseButton = new createjs.MovieClip();
    this.pauseButton.addChild(this.pauseImage);
    this.mainGame.stage.addChild(this.pauseButton);
    this.pauseButton.addEventListener("click",()=>this.pauseGame());
  }

  public DestroyGameUI()
  {
    this.pauseButton.removeChild(this.pauseImage);
    this.mainGame.stage.removeChild(this.pauseButton);
    this.pauseButton.removeEventListener("click",()=>this.pauseGame());
  }

  public pauseGame()
  {
    this.DestroyGameUI();
    this.mainGame.handlePause();
    this.CallPauseScreen();
    this.mainGame.stage.update();
  }

  public CallPauseScreen()
  {
    this.whiteBorder = new createjs.Bitmap(PreloadGame.queue.getResult("white-border"));
    this.whiteBorder.scaleX = MainGame.GameWidth/this.whiteBorder.image.width;
    this.whiteBorder.scaleY = this.whiteBorder.scaleX;
    this.mainGame.stage.addChild(this.whiteBorder);

    this.pausePanel = new createjs.Bitmap(PreloadGame.queue.getResult("pause-panel"));
    this.pausePanel.scaleY = (MainGame.GameHeight - MainGame.GameHeight/4)/this.pausePanel.image.height;
    this.pausePanel.scaleX = this.pausePanel.scaleY;

    var widthPanel:number = this.pausePanel.image.width * this.pausePanel.scaleX;
    var heightPanel:number = this.pausePanel.image.height * this.pausePanel.scaleY;


    this.pausePanel.x = (MainGame.GameWidth - widthPanel)/2;
    this.pausePanel.y = (MainGame.GameHeight - heightPanel)/2;

    this.mainGame.stage.addChild(this.pausePanel);

    this.continueImage = new createjs.Bitmap(PreloadGame.queue.getResult("continue"));
    this.continueImage.scaleX = this.pausePanel.scaleX;
    this.continueImage.scaleY = this.continueImage.scaleX;

    var heightContinue:number = this.continueImage.image.height*this.continueImage.scaleY;


    this.continueImage.x = this.pausePanel.x + widthPanel/10;
    this.continueImage.y = this.pausePanel.y + heightPanel - heightContinue - heightPanel/10;
    this.mainGame.stage.addChild(this.continueImage);

    this.homeImage = new createjs.Bitmap(PreloadGame.queue.getResult("home"));
    this.homeImage.scaleX = this.continueImage.scaleX;
    this.homeImage.scaleY = this.homeImage.scaleX;

    var heightHome:number = this.homeImage.image.height * this.homeImage.scaleY;
    var widthHome:number = this.homeImage.image.width * this.homeImage.scaleX;
    this.homeImage.y = this.pausePanel.y + heightPanel - heightHome - heightPanel/10;
    this.homeImage.x = widthPanel + this.pausePanel.x - widthHome - widthPanel/10;
    this.mainGame.stage.addChild(this.homeImage);

    this.continueImage.addEventListener("click",()=>this.clickContinue());
    this.homeImage.addEventListener("click",()=>this.clickHome());


  }
  public clickContinue()
  {
    this.DestroyPauseScreen();
    this.mainGame.handleResume();
    this.CallGameUi();
  }
  public clickHome()
  {
    window.location.href = MainGame.LogOutUrl;
  }
  public startGame()
  {
    this.DestroyMainMenu();
    this.mainGame.StartPlayGame();
  }
  public reposisi():void
  {
    this.logoImage.x = MainGame.GameWidth/20;
    this.logoImage.y = MainGame.GameHeight/20;
    this.logo2Image.x = this.logoImage.x+(this.logoImage.image.width*this.logoImage.scaleX)+20;
    this.logo2Image.y = MainGame.GameHeight/2 - (this.logo2Image.image.height/2 * this.logo2Image.scaleY);
    this.mainButton.x = this.logo2Image.x + (this.logo2Image.image.width * this.logo2Image.scaleX) + ((MainGame.GameWidth - (this.logo2Image.x + (this.logo2Image.image.width * this.logo2Image.scaleX)))/2) - (this.mainImage.image.width * this.mainImage.scaleX * 0.5);
    this.mainButton.y = (this.logo2Image.image.height*this.logo2Image.scaleY)/2+this.logo2Image.y;
  }
  public DestroyMainMenu():void
  {

    this.mainButton.removeEventListener("click",()=>this.startGame());
    this.mainButton.removeChild(this.mainImage);
    this.stage.removeChild(this.mainButton);
    this.stage.removeChild(this.logo2Image);
  }



  public DestroyPauseScreen():void
  {
    this.continueImage.removeEventListener("click",()=>this.clickContinue());
    this.homeImage.removeEventListener("click",()=>this.clickHome());
    this.mainGame.stage.removeChild(this.homeImage);
    this.mainGame.stage.removeChild(this.pausePanel);
    this.mainGame.stage.removeChild(this.whiteBorder);
    this.mainGame.stage.removeChild(this.continueImage);
  }
}