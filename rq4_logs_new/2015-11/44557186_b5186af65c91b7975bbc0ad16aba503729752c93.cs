using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System.Threading;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Media;
namespace COSC438GameDesignNewREV
{ 
    public enum GameState
    {
        StartMenu,
        ControlMenu,
        DZMenu,
        CurrentMenu,
        MedicLvLMenu,
        Loading,
        Playing,
        Dead,
        LVL0,
        LVL1,
        LVL2,
        LVL3,
        LVL4,
        LVL5,
        Paused
    }
    public class Game1 : Game
    {
        /*
        http://www.spikie.be/blog/page/Building-a-main-menu-and-loading-screens-in-XNA.aspx
        //Load Screen,Pause,Main Screen code pretty much straight from a tutorial
        */
        private SpriteFont text;
        private SpriteFont ohmText;
        private SoundEffect rockFall;
        private SoundEffect jumpSound;
        private SoundEffect cavein;
        private SoundEffect medkit;
        private SoundEffect pickaxe;
        private SoundEffect electric;
        private SoundEffect crane;
        private SoundEffect burstSound;
        //private SoundEffect backgroundMusic;
        private Song backgroundMusic;
        //Screen Images
        private Texture2D MinerDispTexture;
        private Texture2D EngDispTexture;
        private Texture2D ForemanDispTexture;
        private Texture2D pauseScreen;
        private Texture2D loadScreen;
        private Texture2D interactionPrompt;
        private Texture2D startScreen;
        private Texture2D controlScreen;
        private Texture2D DZMenu;
        private Texture2D CurrentMenu;
        private Texture2D DeadMenu;
        private Texture2D LVL0Screen;
        private Texture2D LVL1Screen;
        private Texture2D LVL2Screen;
        private Texture2D LVL3Screen;
        //Screen locations
        private Vector2 textBox;
        private Vector2 sButtonPos;
        private Vector2 eButtonPos;
        private Vector2 rButtonPos;
        private Vector2 controlButtonPos;    
        //Dimensions
        private const float oWidth = 50f;
        private const float oHeight = 50f;
        private Thread backgroundThread;
        private bool isLoading = false;
        private bool restart = false;
        private bool firstLoad = true;
        private MouseState mouseState;
        private MouseState prevMouseState;
        private KeyboardState keyState;
        private Rectangle playerBox;
        private Rectangle sButtonBox;
        private Rectangle eButtonBox;
        private Rectangle rButtonBox;
        private Rectangle infoButtonBox;
        private Rectangle controlButtonBox;
        public SoundEffect BurstSound
        {
            get
            {
                return burstSound;
            }
            set
            {
                burstSound = value;
            }
        }
        public Song BackgroundMusic
        {
            get
            {
                return backgroundMusic;
            }
            set
            {
                backgroundMusic = value;
            }
        }
        public SoundEffect JumpSound
        {
            get
            {
                return jumpSound;
            }
            set
            {
                jumpSound = value;
            }

        }
        public SoundEffect Cavein
        {
            get
            {
                return cavein;
            }
            set
            {
                cavein = value;
            }
        }
        public SoundEffect RockFall
        {
            get
            {
                return rockFall;
            }
            set
            {
                rockFall = value;
            }
        }
        public SoundEffect Medkit
        {
            get
            {
                return medkit;
            }
            set
            {
                medkit = value;
            }

        }
        public SoundEffect Pickaxe
        {
            get
            {
                return pickaxe;
            }
            set
            {
                pickaxe = value;
            }
        }
        public SoundEffect Electric
        {
            get
            {
                return electric;
            }
            set
            {
                electric = value;
            }
        }
        public SoundEffect Crane
        {
            get
            {
                return crane;
            }
            set
            {
                crane = value;
            }

        }
        public GameState GetGameState
        {
            get
            {
                return gameState;
            }
            set
            {
                gameState = value;
            }
        }
        public SpriteBatch SpriteBatchDraw
        {
            get
            {
                return spriteBatch;
            }
            set
            {
                spriteBatch = value;
            }
        }
        public Rectangle PlayerBox
        {
            get
            {
                return playerBox;
            }
            set
            {
                playerBox = value;
            }
        }
        public Thread BackgroundThread
        {
            get
            {
                return backgroundThread;
            }
            set
            {
                backgroundThread = value;
            }
        }
        public MouseState MouseState
        {
            get
            {
                return mouseState;
            }
            set
            {
                mouseState = value;
            }
        }
        public MouseState PrevMouseState
        {
            get
            {
                return prevMouseState;
            }
            set
            {
                prevMouseState = value;
            }
        }
        public void LoadGame()
        {      
            keyState = new KeyboardState();                
            if (prevGameState == GameState.Dead)
            {
                replay();
                gameState = GameState.Playing;
                isLoading = false;
            }
            else if (!restart)
            {
                Thread.Sleep(700);
                gameState = GameState.Playing;
                isLoading = false;
            }
        }
        public void replay()
        {
            restart = true;
            prevGameState = GameState.Playing;
            Initialize();
            LoadContent();
        }
        public GameState findGameState()
        {
            switch(physicsEngine.ACTIVELEVELFunc)
            {
                case 0:
                    {
                        return GameState.LVL0;               
                    }
                case 1:
                    {
                        return GameState.LVL1;                  
                    }
                case 2:
                    {
                        return GameState.LVL2;
                    }
                case 3:
                    {
                        return GameState.LVL3;
                    }
                case 4:
                    {
                        return GameState.LVL4;
                    }
                case 5:
                    {
                        return GameState.LVL5;
                    }
            }
            return GameState.LVL1;
        }
        public void MouseClicked(int x, int y)
        {           
            Rectangle mouseClickBox = new Rectangle(x, y, 10, 10);
            if(gameState == GameState.StartMenu)
            {            
                prevGameState = GameState.StartMenu;
                if(mouseClickBox.Intersects(sButtonBox))
                {               
                    gameState = GameState.Loading;
                    isLoading = false;                   
                }
                if(mouseClickBox.Intersects(eButtonBox))
                {
                    Exit();
                }
                if (mouseClickBox.Intersects(controlButtonBox))
                {
                    gameState = GameState.ControlMenu;
                }
            }  
            if(gameState == GameState.Playing)
            {
                if(mouseClickBox.Intersects(infoButtonBox))
                {
                    gameState = findGameState();
                }
            }         
            if(gameState == GameState.Paused)
            {
                prevGameState = GameState.Paused;
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if(mouseClickBox.Intersects(rButtonRec))
                {
                    
                    gameState = GameState.Playing;
                }
                if (mouseClickBox.Intersects(eButtonBox))
                {
                    Exit();
                }
                if(mouseClickBox.Intersects(controlButtonBox))
                {
                   
                    gameState = GameState.ControlMenu;
                }
            }
            if (gameState == GameState.ControlMenu)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if (mouseClickBox.Intersects(rButtonRec))
                {
                    prevGameState = GameState.ControlMenu;
                    gameState = GameState.Playing;
                }              
            }
            if (gameState == GameState.DZMenu)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if (mouseClickBox.Intersects(rButtonRec))
                {
                    prevGameState = GameState.DZMenu;
                    gameState = GameState.Playing;
                    int temp = physicsEngine.itemIdx(0);
                    if (temp != -1)
                    {
                        InventoryContainer.RemoveAt(temp);
                    }
                    physicsEngine.ACTIVELEVELFunc = 6;
                    physicsEngine.EventStates[1] = true;
                    physicsEngine.loadLevel(1);                  
                    physicsEngine.dangerZoneEvent();
                }
            }
            if (gameState == GameState.CurrentMenu)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if (mouseClickBox.Intersects(rButtonRec))
                {
                    prevGameState = GameState.ControlMenu;
                    physicsEngine.ACTIVELEVELFunc = 7;
                    physicsEngine.EventStates[2] = true;
                    physicsEngine.loadLevel(1);
                    gameState = GameState.Playing;
                }
            }
            if (gameState == GameState.MedicLvLMenu)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if (mouseClickBox.Intersects(rButtonRec))
                {
                    prevGameState = GameState.MedicLvLMenu;
                    gameState = GameState.Playing;
                }
            }
            if (gameState == GameState.Dead)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if (mouseClickBox.Intersects(rButtonRec))
                {
                    if (physicsEngine.ACTIVELEVELFunc == 6)
                    {
                        prevGameState = GameState.Dead;
                        gameState = GameState.DZMenu;
                    }
                    else if (physicsEngine.ACTIVELEVELFunc == 7)
                    {
                        prevGameState = GameState.Dead;
                        gameState = GameState.ControlMenu;
                    }
                    else if (physicsEngine.ACTIVELEVELFunc == 8)
                    {

                    }
                    else
                    {
                        prevGameState = GameState.Dead;
                        gameState = GameState.Loading;
                        isLoading = false;
                    }          
                }
                if (mouseClickBox.Intersects(eButtonBox))
                {
                    Exit();
                }
            }
            if (gameState == GameState.LVL0 || gameState == GameState.LVL1 || gameState == GameState.LVL2 || gameState == GameState.LVL3
                || gameState == GameState.LVL4 || gameState == GameState.LVL5)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 30);
                if (mouseClickBox.Intersects(rButtonRec))
                {
                    gameState = GameState.Playing;
                }
            }           
        }
        //Grid give us our collision boxes and sprite images
        private GridLayout[] grid;
        private GridLayout[] shadeGrid;
        //All statically placed sprites are defined in MapContainer.cs
        //Point class defines our characters position at all times
        public class Point
        {
            public int X { get; set; }
            public int Y { get; set; }
        }
        //MonoStuff
        private PlayerSpriteClass ptrSwapClass;
        private PlayerSpriteClass playerClass;
        private PlayerSpriteClass playerClassMiner;
        private PlayerSpriteClass playerClassEng;
        private PlayerSpriteClass playerClassFore;
        private GraphicsDeviceManager graphics;
        private SpriteBatch spriteBatch;
        //Phyics Engine handles all movement, interaction, and collision
        private Physics physicsEngine;
        //MISC Constants. These constants are soley used for dictonary bounds
        private const int MAPSIZE = 240;
        private const int STATICSSIZE = 100;
        private const int STATICOFFSET = 4;
        private const int eventTime = 50;
        //Used for game state interactions and displaying inventory panel
        private List<ItemTile> inventoryContainer;
        //Dynamic sprites
        private Texture2D activePlayer;
        private Texture2D cursorDummyPassive;
        //Coordinates for keyboard and mouse
        private int spriteSourceX, spriteSourceY;
        private Point currMousePosition;
        private Point currPlayerPosition;
        private GameState gameState;
        private GameState prevGameState;
        //Getters & Setters
        public PlayerSpriteClass PtrSwapClass
        {
            get
            {
                return ptrSwapClass;
            }
            set
            {
                ptrSwapClass = value;
            }
        }
        public PlayerSpriteClass PlayerClass
        {
            get
            {
                return playerClass;
            }
            set
            {
                playerClass = value;  
            }
        }
        public PlayerSpriteClass PlayerClassMiner
        {
            get
            {
                return playerClassMiner;
            }
            set
            {
                playerClassMiner = value;
            }
        }
        public PlayerSpriteClass PlayerClassEng
        {
            get
            {
                return playerClassEng;
            }
            set
            {
                playerClassEng = value;
            }
        }
        public PlayerSpriteClass PlayerClassFore
        {
            get
            {
                return playerClassFore;
            }
            set
            {
                playerClassFore = value;
            }
        }
        public int SpriteSourceX
        {
            get
            {
                return spriteSourceX;
            }
            set
            {
                spriteSourceX = value;
            }
        }
        public int SpriteSourceY
        {
            get
            {
                return spriteSourceY;
            }
            set
            {
                spriteSourceY = value;
            }
        }
        public Texture2D CursorDummyPassive
        {
            get
            {
                return cursorDummyPassive;
            }
            set
            {
                cursorDummyPassive = value;
            }
        }
        public SpriteBatch SpriteBatch
        {
            get
            {
                return spriteBatch;
            }
            set
            {
                spriteBatch = value;
            }
        }
        public Texture2D ActivePlayer
        {
            get
            {
                return activePlayer;
            }
            set
            {
                activePlayer = value;
            }
        }
        public GraphicsDeviceManager graphicsFunc
        {
            get
            {
                return graphics;
            }
        }
        public Texture2D activePlayerFunc
        {
            get
            {
                return activePlayer;
            }
            set
            {
                activePlayer = value;
            }
        }
        public Point currPlayerPositionFunc
        {
            get
            {
                return currPlayerPosition;
            }
        }
        public Point currMousePositionFunc
        {
            get
            {
                return currMousePosition;
            }
        }
        public GraphicsDeviceManager Graphics
        {
            get
            {
                return graphics;
            }
            set
            {
                graphics = value;
            }
        }
        public GridLayout[] Grid
        {
            get
            {
                return grid;
            }
            set
            {
                grid = value;
            }
        }
        public GridLayout[] GenMaps
        {
            get
            {
                return grid;
            }
        }
        public GridLayout[] ShadeGrid
        {
            get
            {
                return shadeGrid;
            }
            set
            {
                shadeGrid = value;
            }
        }
        public Physics PhysicsEngine
        {
            get
            {
                return physicsEngine;
            }
        }
        public List<ItemTile> InventoryContainer
        {
            get
            {
                return inventoryContainer;
            }
            set
            {
                inventoryContainer = value;
            }
        }
        public int getInvItem(int key)
        {
            for(int x = 0; x < inventoryContainer.Count; x++)
            {
                if(inventoryContainer[x].ItemKey.Equals(key))
                {
                    return x;
                }
            }
            return -1;
        }
        //Constructor
        public Game1()
        {                           
            graphics = new GraphicsDeviceManager(this); 
            Content.RootDirectory = "Content";               
        }  
        protected override void Initialize()
        {
            this.IsMouseVisible = true;
            inventoryContainer = new List<ItemTile>();
            currPlayerPosition = new Point();
            currMousePosition = new Point();
            //Bounding Box Of GUI Buttons       
            sButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - 50, 150);
            rButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - 50, 150);
            eButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - 50, 250);
            controlButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - 50, 200);
            //Bounding Box Of GUI Buttons
            sButtonBox = new Rectangle((int)sButtonPos.X, (int)sButtonPos.Y, 150, 30);
            rButtonBox = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 150, 30);
            eButtonBox = new Rectangle((int)eButtonPos.X, (int)eButtonPos.Y, 150, 30);
            infoButtonBox = new Rectangle(760, 40, 40, 40);
            controlButtonBox = new Rectangle((int)controlButtonPos.X, (int)controlButtonPos.Y, 150, 30);                              
            //Load all levels of the game
            grid = new GridLayout[15];
            shadeGrid = new GridLayout[15];
            for (int x = 0; x < 15; x++)
            {
                grid[x] = new GridLayout(this);
                shadeGrid[x] = new GridLayout(this);
                //Generate grids based off layout matricies
                grid[x].generateGrid(x);
                foreach (ItemTile tile in grid[x].ItemTile)
                {                  
                    inventoryContainer.Add(tile);
                }              
                shadeGrid[x].generateGrid(-199);
            }           
            mouseState = Mouse.GetState();
            prevMouseState = mouseState;
            spriteSourceX = 0;
            spriteSourceY = 0;
            base.Initialize();
        }
        protected override void LoadContent()
        {
            text = Content.Load<SpriteFont>("text");
            ohmText = Content.Load<SpriteFont>("OhmText");
            //Load music file into a song var
            /*
            this.backgroundMusic = Content.Load<Song>("BackgroundMusic");
            this.jumpSound = Content.Load<SoundEffect>("jump");
            this.cavein = Content.Load<SoundEffect>("RockSlide");
            this.medkit = Content.Load<SoundEffect>("medicine");
            this.pickaxe = Content.Load<SoundEffect>("Pickaxe");
            this.electric = Content.Load<SoundEffect>("electric");
            this.crane = Content.Load<SoundEffect>("crane");
            this.rockFall = Content.Load<SoundEffect>("rockFall");
            this.burstSound = Content.Load<SoundEffect>("BurstSound");
            // this.backgroundMusic = Content.Load<SoundEffect>("BackgroundMusic");          
            MediaPlayer.Play(backgroundMusic);
            */
            textBox = new Vector2(80, 80);           
            loadScreen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/LoadingScreen.gif"));
            startScreen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/StartMenu.png"));
            pauseScreen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/PauseMenu.png"));
            controlScreen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/ControlsMenu.png"));
            CurrentMenu = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/CurrentlyOutofOrderMenu.png"));
            DZMenu = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/DangerZoneMenu.png"));
            DeadMenu = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/DeathScreen.png"));
            LVL0Screen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/InfoScreenLevel0.png"));
            LVL1Screen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/InfoScreenLevel1.png"));
            LVL2Screen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/InfoScreenLevel2.png"));
            LVL3Screen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/GUIImages/InfoScreenLevel3.png"));          
            //////////////////////PRE GAME LOADING           
            currPlayerPosition.X = 10;
            currPlayerPosition.Y = graphics.PreferredBackBufferHeight;
            spriteBatch = new SpriteBatch(GraphicsDevice);
            /*
            Generate the initial physicsEngine, this engine processes all movement and collision detection for each map based on the collisionBox for that specificed map
            All events are handled within the physicsEngine, these events update container objects from this class, so be careful with what you modify.
            */
            physicsEngine = new Physics(this, 1, spriteBatch, graphics);
            physicsEngine.loadLevel(1);
            if (restart)
            {
                restart = false;
                LoadGame();
            }
            else if (firstLoad)
            {
                gameState = GameState.StartMenu;
                prevGameState = GameState.StartMenu;
                firstLoad = false;
            }
            else
            {
                gameState = GameState.Playing;
            }          
            MinerDispTexture = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/PlayerImages/MinerSpriteSheet.png"));
            EngDispTexture = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/PlayerImages/EngineerSpriteSheet.png"));
            ForemanDispTexture = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/PlayerImages/ForemanSpriteSheet.png"));
            //Load all of our possible classes, player class is always the active one to be switched with later
            playerClass = new PlayerSpriteClass(MinerDispTexture, 3, 2, 1, new Vector2(currPlayerPosition.X, currPlayerPosition.Y - 80), this);
            playerClassMiner = new PlayerSpriteClass(MinerDispTexture, 3, 2, 1, new Vector2(currPlayerPosition.X, currPlayerPosition.Y - 80), this);
            playerClassEng = new PlayerSpriteClass(EngDispTexture, 3, 2, 2, new Vector2(50, Graphics.PreferredBackBufferHeight - 120), this);
            playerClassFore = new PlayerSpriteClass(ForemanDispTexture, 3, 2, 4, new Vector2(400,Graphics.PreferredBackBufferHeight - 80), this);
            //Since the games coded with dims of activePlayer, this texture is a dummy texture for the size of the player 
            //(to avoid redoing alot of code)
            activePlayer = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/PlayerImages/P1.png"));
            //////////////////////END PRE GAME LOADING
        }
        //TODO Do we need this for our game? :/
        protected override void UnloadContent()
        {
        }       
        protected override void Update(GameTime gameTime)
        {
            keyState = Keyboard.GetState();
            if(physicsEngine.ACTIVELEVELFunc > 5)
            {
                MediaPlayer.Pause();
            }
            else
            {
                MediaPlayer.Resume();
            }
            if (gameState == GameState.Loading && !isLoading)
            {
                backgroundThread = new Thread(LoadGame);
                isLoading = true;
                backgroundThread.Start();
            }
            mouseState = Mouse.GetState();           
            if (prevMouseState.LeftButton == ButtonState.Pressed && mouseState.LeftButton == ButtonState.Released)
            {              
                MouseClicked(mouseState.X, mouseState.Y);
            }
            prevMouseState = mouseState;
            if(gameState == GameState.Playing && isLoading)
            {
                LoadGame();
                isLoading = false;                           
            }            
            if (gameState == GameState.Playing)
            {
                if (keyState.IsKeyDown(Keys.P))
                {
                    prevGameState = GameState.Playing;
                    gameState = GameState.Paused;
                }              
                //Runs all methods relted to handling user input events.
                //ProcessInputFunctions has a chaining effect to all other detections
                playerBox = new Rectangle(currPlayerPosition.X, currPlayerPosition.Y, 40, 80);
                physicsEngine.ProcessInputFunctions(Mouse.GetState(), Keyboard.GetState(), gameTime);
            }                       
            base.Update(gameTime);
        }
        protected override void Draw(GameTime gameTime)
        {
            spriteBatch.Begin();
            if(gameState == GameState.StartMenu)
            {
                spriteBatch.Draw(startScreen, new Rectangle(0,0,graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight),Color.White);            
            }
            if(gameState == GameState.Loading && physicsEngine.ACTIVELEVELFunc == 6)
            {
                spriteBatch.Draw(loadScreen, new Vector2((GraphicsDevice.Viewport.Width / 2) - (loadScreen.Width / 2), (GraphicsDevice.Viewport.Height / 2) - (loadScreen.Height / 2)), Color.White);
            }
            if (gameState == GameState.Loading)
            {
                spriteBatch.Draw(loadScreen, new Vector2((GraphicsDevice.Viewport.Width / 2) - (loadScreen.Width / 2), (GraphicsDevice.Viewport.Height / 2) - (loadScreen.Height / 2)), Color.White);
            }
            if (gameState == GameState.Playing)
            {
                //Load sprites
                drawSprites();
                //Load mouse and player sprites
                //spriteBatch.Draw(activePlayer, new Rectangle(currPlayerPosition.X, currPlayerPosition.Y - ActivePlayer.Height, ActivePlayer.Width, ActivePlayer.Height), Color.White);
                //spriteBatch.Draw(cursorDummy, new Rectangle(currMousePosition.X, currMousePosition.Y, 15, 15), new Rectangle(spriteSourceX, spriteSourceY, 40, 80), Color.White);
                playerClass.PlayerLocation = new Vector2(currPlayerPosition.X, currPlayerPosition.Y);
                playerClass.ACTIVELVL = physicsEngine.ACTIVELEVELFunc;
                playerClass.Draw(spriteBatch, new Vector2(currPlayerPosition.X, currPlayerPosition.Y - ActivePlayer.Height));                
                //spriteBatch.Draw(activePlayerDisp, new Rectangle(currPlayerPosition.X, currPlayerPosition.Y - ActivePlayer.Height, ActivePlayer.Width, ActivePlayer.Height), new Rectangle(spriteSourceX, spriteSourceY, 40, 80), Color.White);
                /*
                NOTE TDOWNFunc only returns true when all conditions are met within the physics engine.
                */
                if (physicsEngine.TDOWNFunc && !physicsEngine.EventStates[1])
                {
                    physicsEngine.checkTrajectoryEvent1();                
                }
                if (inventoryContainer[0].Equipt)
                    {
                        drawFunc(physicsEngine.PathHighlight, physicsEngine.ShowBox,physicsEngine.DrawAngel);
                    }
                if (physicsEngine.ACTIVELEVELFunc == 6)
                {
                    physicsEngine.dangerZoneSimulation();
                }
                if(physicsEngine.InterActiveProximity && !physicsEngine.EventStates[2])
                {
                    spriteBatch.Draw(interactionPrompt, new Rectangle(160, graphics.PreferredBackBufferHeight - (160 + interactionPrompt.Height), interactionPrompt.Width, interactionPrompt.Height),Color.White);
                }

                if(physicsEngine.ACTIVELEVELFunc == 7)
                {
                    foreach (OhmTile tile in grid[physicsEngine.ACTIVELEVELFunc].OhmTile)
                    {
                        Color temp = Color.White;
                        switch (tile.Colour)
                        {
                            case 1:
                                {
                                    temp = Color.Green;
                                    break;
                                }
                            case 2:
                                {
                                    temp = Color.Red;
                                    break;
                                }
                            case 3:
                                {
                                    temp = Color.Yellow;
                                    break;
                                }
                        }
                        spriteBatch.DrawString(ohmText, tile.Ohmval.ToString() + "Ohms", new Vector2(tile.Box.X, tile.Box.Y), temp);
                        //Display For Wire Voltage
                        spriteBatch.DrawString(text, "240V", new Vector2(20, 140), Color.Black);
                        spriteBatch.DrawString(text, "480V", new Vector2(20, 260), Color.Black);
                        spriteBatch.DrawString(text, "600V", new Vector2(20, 380), Color.Black);
                        //Display For End Wire Current
                        //Green
                        if (physicsEngine.ActivePlayer == ActivePlayerz.Engineer)
                        {
                            String overRide;
                            if (physicsEngine.ResList[0] > 999999)
                            {
                                overRide = "0.0";
                            }
                            else
                            {
                                overRide = (Math.Truncate(physicsEngine.ResList[0] * 100) / 100).ToString();
                                if (Double.Parse(overRide) >= 2.0)
                                {
                                    overRide = "High Current \n" + overRide;
                                }
                                else if (Double.Parse(overRide) < 1.5)
                                {
                                    overRide = "Low Current \n" + overRide;
                                }
                                else
                                {
                                    overRide = "Valid Current \n" + overRide;
                                }
                            }
                            spriteBatch.DrawString(text, overRide + "A", new Vector2(600, 140), Color.Black);
                            /*
                        else if (physicsEngine.dispLocation(1) == 1)
                        {
                            spriteBatch.DrawString(text, physicsEngine.ResList[0].ToString() + "A", new Vector2(280, 190), Color.Black);
                        }
                             * */
                            //Red    
                            if (physicsEngine.ResList[1] > 999999)
                            {
                                overRide = "0.0";
                            }
                            else
                            {
                                overRide = (Math.Truncate(physicsEngine.ResList[1] * 100) / 100).ToString();
                                if (Double.Parse(overRide) > 2)
                                {
                                    overRide = "High Current \n" + overRide;
                                }
                                else if (Double.Parse(overRide) < 1.5)
                                {
                                    overRide = "Low Current \n" + overRide;
                                }
                                else
                                {
                                    overRide = "Valid Current \n" + overRide;
                                }
                            }
                            spriteBatch.DrawString(text, overRide + "A", new Vector2(600, 265), Color.Black);
                            /*
                        else if (physicsEngine.dispLocation(2) == 1)
                        {
                            spriteBatch.DrawString(text, physicsEngine.ResList[1].ToString() + "A", new Vector2(280, 290), Color.Black);
                        }
                             * */
                            //Yellow
                            if (physicsEngine.ResList[2] > 999999)
                            {
                                overRide = "0.0";
                            }
                            else
                            {
                                overRide = (Math.Truncate(physicsEngine.ResList[2] * 100) / 100).ToString();
                                if (Double.Parse(overRide) > 2)
                                {
                                    overRide = "High Current \n" + overRide;
                                }
                                else if (Double.Parse(overRide) < 1.5)
                                {
                                    overRide = "Low Current \n" + overRide;
                                }
                                else
                                {
                                    overRide = "Valid Current \n" + overRide;
                                }
                            }
                            spriteBatch.DrawString(text, overRide + "A", new Vector2(600, 380), Color.Black);
                            /*
                        else if (physicsEngine.dispLocation(3) == 1)
                        {
                            spriteBatch.DrawString(text, physicsEngine.ResList[2].ToString() + "A", new Vector2(280, 402), Color.Black);
                        }
                             * */
                        }
                    }              
                    physicsEngine.ohmCollisionDetection();
                    physicsEngine.scoreWires();
                }
            }  
            if (gameState == GameState.Paused)
            {         
                spriteBatch.Draw(pauseScreen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.ControlMenu)
            {
                spriteBatch.Draw(controlScreen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.DZMenu)
            {
                spriteBatch.Draw(DZMenu, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.CurrentMenu)
            {
                spriteBatch.Draw(CurrentMenu, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.Dead)
            {
                spriteBatch.Draw(DeadMenu, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.LVL0)
            {
                spriteBatch.Draw(LVL0Screen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.LVL1)
            {
                spriteBatch.Draw(LVL1Screen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.LVL2)
            {
                spriteBatch.Draw(LVL2Screen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.LVL3)
            {
                spriteBatch.Draw(LVL3Screen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.LVL4)
            {
                //spriteBatch.Draw(LVL4Screen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            if (gameState == GameState.LVL5)
            {
                //spriteBatch.Draw(LVL5Screen, new Rectangle(0, 0, graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);
            }
            spriteBatch.End();
            base.Draw(gameTime);
        }
        //Used for drawing throw projection of the mining axe
        public void drawFunc(Texture2D T,Rectangle R,float drawAngel)
        {
            Console.WriteLine(mouseState.X);
            Console.WriteLine(currPlayerPosition.X);
            if (physicsEngine.CharacterFacing == CharDirection.Right && mouseState.X > currPlayerPosition.X)
            {              
                spriteBatch.Draw(T, new Vector2(currPlayerPosition.X + playerBox.Width + 15, currPlayerPosition.Y - playerBox.Height / 2 - 15), R, Color.White, drawAngel, new Vector2(0, 0), 1.0f, SpriteEffects.None, 1);    
            }
            else if (physicsEngine.CharacterFacing == CharDirection.Left && mouseState.X < currPlayerPosition.X)
            {
                spriteBatch.Draw(T, new Vector2(currPlayerPosition.X, currPlayerPosition.Y - playerBox.Height / 2 - 15), R, Color.White, drawAngel, new Vector2(0, 0), 1.0f, SpriteEffects.None, 1);
            }                         
        }
        public void drawSprites()
        {
            foreach (BackGroundTile tile in grid[physicsEngine.ACTIVELEVELFunc].BackGroundTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }            
            foreach (CaveTopTile tile in grid[physicsEngine.ACTIVELEVELFunc].CaveTopTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (CraneTile tile in grid[physicsEngine.ACTIVELEVELFunc].CraneTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (CollisionTile tile in grid[physicsEngine.ACTIVELEVELFunc].CollisionTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (ItemTile tile in grid[physicsEngine.ACTIVELEVELFunc].ItemTile)
            {
                if (!tile.Collected)
                {
                    spriteBatch.Draw(tile.Image, tile.Box, Color.White);
                }
            }
            foreach (PlatFormTile tile in grid[physicsEngine.ACTIVELEVELFunc].PlatFormTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (LadderTile tile in grid[physicsEngine.ACTIVELEVELFunc].LadderTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (SlipperyTile tile in grid[physicsEngine.ACTIVELEVELFunc].SlipperyTiles)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (CrackedTile tile in grid[physicsEngine.ACTIVELEVELFunc].CrackedTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (OhmDropTile tile in grid[physicsEngine.ACTIVELEVELFunc].OhmDropTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (WireTile tile in grid[physicsEngine.ACTIVELEVELFunc].WireTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (OhmTile tile in grid[physicsEngine.ACTIVELEVELFunc].OhmTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (SlipperyTile tile in grid[physicsEngine.ACTIVELEVELFunc].SlipperyTiles)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (StatusTile tile in grid[physicsEngine.ACTIVELEVELFunc].StatusTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }                     
            if (physicsEngine.EventStates[1] == true && physicsEngine.ACTIVELEVELFunc >= 0 && physicsEngine.ACTIVELEVELFunc <= 5)
            {
                DrawShaders();
            }
            if (physicsEngine.ActivePlayer == ActivePlayerz.Miner)
            {
                if (playerClassEng.ACTIVELVL == physicsEngine.ACTIVELEVELFunc)
                {
                    playerClassEng.Draw(spriteBatch, playerClassEng.PlayerLocation);
                }
                else if (playerClassFore.ACTIVELVL == physicsEngine.ACTIVELEVELFunc)
                {
                    playerClassFore.Draw(spriteBatch, playerClassFore.PlayerLocation);
                }
            }
            else if (physicsEngine.ActivePlayer == ActivePlayerz.Engineer)
            {
                if (playerClassMiner.ACTIVELVL == physicsEngine.ACTIVELEVELFunc)
                {
                    playerClassMiner.Draw(spriteBatch, playerClassMiner.PlayerLocation);
                }
                else if (playerClassFore.ACTIVELVL == physicsEngine.ACTIVELEVELFunc)
                {
                    playerClassFore.Draw(spriteBatch, playerClassFore.PlayerLocation);
                }
            }
            else if (physicsEngine.ActivePlayer == ActivePlayerz.Foreman)
            {
                if (playerClassMiner.ACTIVELVL == physicsEngine.ACTIVELEVELFunc)
                {
                    playerClassMiner.Draw(spriteBatch, playerClassMiner.PlayerLocation);
                }
                else if (playerClassEng.ACTIVELVL == physicsEngine.ACTIVELEVELFunc)
                {
                    playerClassEng.Draw(spriteBatch, playerClassEng.PlayerLocation);
                }
            }
            foreach (Tile tile in grid[physicsEngine.ACTIVELEVELFunc].GuiTiles)
            {
                if (tile.Active)
                {
                    spriteBatch.Draw(tile.Image, tile.Box, Color.White);
                }
            }
            //Display for heatlh reamining
           spriteBatch.DrawString(text, (physicsEngine.HealthRemaining / 2).ToString() + "%", new Vector2(70, 5), Color.Black);
           spriteBatch.DrawString(text, (physicsEngine.ACTIVELEVELFunc + "LVL"), new Vector2(10, 5), Color.Black);     
        }
        public void DrawShaders()
        {
            foreach(ShadedTile tile in shadeGrid[physicsEngine.ACTIVELEVELFunc].ShadeTile)
            {
                Rectangle playerShadeBox = new Rectangle(currPlayerPosition.X - (activePlayer.Width / 2) * 6, currPlayerPosition.Y  - ( (activePlayer.Height / 2) * 3) - physicsEngine.YOffSet, activePlayer.Height * 3, activePlayer.Width * 6);
                Rectangle playerShadeBox2 = new Rectangle(currPlayerPosition.X - (activePlayer.Width / 2) * 4, currPlayerPosition.Y - ((activePlayer.Height / 2) * 2) - physicsEngine.YOffSet, activePlayer.Height * 2, activePlayer.Width * 4);
                Rectangle playerShadeBox3 = new Rectangle(currPlayerPosition.X - (activePlayer.Width / 2) * 2, currPlayerPosition.Y - ((activePlayer.Height / 2) * 1) - physicsEngine.YOffSet, activePlayer.Height * 1, activePlayer.Width * 2);
                Rectangle playerFlashLightBox = new Rectangle(currPlayerPosition.X, currPlayerPosition.Y, 400, 400);
                if (physicsEngine.CharacterFacing == CharDirection.Right)
                {
                    playerFlashLightBox = new Rectangle(currPlayerPosition.X, currPlayerPosition.Y - 90, 300, 90);
                }
                else if (physicsEngine.CharacterFacing == CharDirection.Left)
                {
                    playerFlashLightBox = new Rectangle(currPlayerPosition.X - 400, currPlayerPosition.Y - 90, 300, 90);
                }               
                if (physicsEngine.FlashLightView)
                {
                    if (tile.Box.Intersects(playerFlashLightBox))
                    {
                        spriteBatch.Draw(tile.Image, tile.Box, Color.White * .10f);
                    }
                    else if (playerShadeBox3.Intersects(tile.Box))
                    {
                        spriteBatch.Draw(tile.Image, tile.Box, Color.White * .25f);
                    }
                    else if (playerShadeBox2.Intersects(tile.Box))
                    {
                        spriteBatch.Draw(tile.Image, tile.Box, Color.White * .50f);
                    }
                    else if (playerShadeBox.Intersects(tile.Box))
                    {
                        spriteBatch.Draw(tile.Image, tile.Box, Color.White * .75f);
                    }
                    else
                    {
                        spriteBatch.Draw(tile.Image, tile.Box, Color.White);
                    }
                }                       
                else if (playerShadeBox3.Intersects(tile.Box))
                {
                     spriteBatch.Draw(tile.Image, tile.Box, Color.White * .25f);
                }
                else if (playerShadeBox2.Intersects(tile.Box))
                {
                     spriteBatch.Draw(tile.Image, tile.Box, Color.White * .50f);
                }
                else if (playerShadeBox.Intersects(tile.Box))
                {
                      spriteBatch.Draw(tile.Image, tile.Box, Color.White * .75f);
                }
                else
                {
                      spriteBatch.Draw(tile.Image, tile.Box, Color.White);
                }   
            }
        }
        public bool checkInventoryItem(int itemNum)
        {
            foreach(ItemTile tile in inventoryContainer)
            {
                if(tile.ItemKey.Equals(itemNum) && tile.Collected && !tile.Consumed && tile.Equipt)
                {
                    return true;
                }
            }
            return false;
        }
        public void overloadedDraw(List<SpikeTile> temp)
        {
            for(int x = 0; x < temp.Count; x++)
            {                          
                spriteBatch.Draw(temp[x].Image, temp[x].Box, Color.White);
            }
        }
    }
}