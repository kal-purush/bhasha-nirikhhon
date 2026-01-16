using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System.Threading;

namespace COSC438GameDesignNewREV
{
    enum GameState
    {
        StartMenu,
        Loading,
        Playing,
        Paused
    }
    public class Game1 : Game
    {
        /*
        http://www.spikie.be/blog/page/Building-a-main-menu-and-loading-screens-in-XNA.aspx
        //Load Screen,Pause,Main Screen code pretty much straight from a tutorial
        */
        //Screen Images
        private Texture2D orb;
        private Texture2D sButton;
        private Texture2D eButton;
        private Texture2D pButton;
        private Texture2D rButton;
        private Texture2D loadScreen;
        private Texture2D interactionPrompt;
        //Screen locations
        private Vector2 orbPos;
        private Vector2 sButtonPos;
        private Vector2 eButtonPos;
        private Vector2 rButtonPos;      
        //Dimensions
        private const float oWidth = 50f;
        private const float oHeight = 50f;
        private Thread backgroundThread;
        private bool isLoading = false;
        private MouseState mouseState;
        private MouseState prevMouseState;
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
            orb = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/orb.jpg"));
            orbPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - (oWidth / 2), (GraphicsDevice.Viewport.Height / 2) - (oHeight / 2));
            pButton = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/Pause.jpg"));
            rButton = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/Resume.jpg"));
            rButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - (rButton.Width / 2), (GraphicsDevice.Viewport.Height / 2) - (rButton.Height / 2));
            interactionPrompt = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/interactionPrompt.png"));
            Thread.Sleep(1000);
            gameState = GameState.Playing;
            isLoading = false;
        }
        public void MouseClicked(int x, int y)
        {
            Rectangle mouseClickBox = new Rectangle(x, y, 10, 10);
            if(gameState == GameState.StartMenu)
            {
                Rectangle sButtonBox = new Rectangle((int)sButtonPos.X, (int)sButtonPos.Y, 100, 20);
                Rectangle eButtonBox = new Rectangle((int)eButtonPos.X, (int)eButtonPos.Y, 100, 20);
                if(mouseClickBox.Intersects(sButtonBox))
                {
                    gameState = GameState.Loading;
                    isLoading = false;
                }
                else if(mouseClickBox.Intersects(eButtonBox))
                {
                    Exit();
                }
            }
            if (gameState == GameState.Playing)
            {
                Rectangle pButtonBox = new Rectangle(graphics.PreferredBackBufferWidth - pButton.Width, 0, 70, 70);
                if(mouseClickBox.Intersects(pButtonBox))
                {
                    gameState = GameState.Paused;
                }
            }
            if(gameState == GameState.Paused)
            {
                Rectangle rButtonRec = new Rectangle((int)rButtonPos.X, (int)rButtonPos.Y, 100, 20);
                if(mouseClickBox.Intersects(rButtonRec))
                {
                    gameState = GameState.Playing;
                }
            }
        }
        //Grid give us our collision boxes and sprite images
        private GridLayout[] grid;
        //All statically placed sprites are defined in MapContainer.cs
        private Dictionary<int, Tuple<int, Texture2D, Rectangle>> STATICSPRITES;
        //Point class defines our characters position at all times
        public class Point
        {
            public int X { get; set; }
            public int Y { get; set; }
        }
        //MonoStuff
        private GraphicsDeviceManager graphics;
        private SpriteBatch spriteBatch;
        //Phyics Engine handles all movement, interaction, and collision
        private Physics physicsEngine;
        //MISC Constants. These constants are soley used for dictonary bounds
        private const int MAPSIZE = 240;
        private const int STATICSSIZE = 100;
        private const int STATICOFFSET = 4;
        private const int eventTime = 50;
        //Timer to slow animations
        private int lastEventTime = 0;
        //sprite Container for map element sprites
        private Dictionary<int, Tuple<int, Texture2D, Rectangle>> spriteContainer;
        //Used for game state interactions and displaying inventory panel
        private Dictionary<int, ItemTile> inventoryContainer;
        //Dynamic sprites
        private Texture2D activePlayer;
        private Texture2D cursorDummyPassive;
        private Texture2D inventory;
        private Texture2D darkbg;
        //Coordinates for keyboard and mouse
        private Point currMousePosition;
        private Point currPlayerPosition;
        private GameState gameState;
        //Getters & Setters
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
        public Dictionary<int, ItemTile> getINV
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
        public Dictionary<int, Tuple<int, Texture2D, Rectangle>> spriteContainerFunc
        {
            get
            {
                return spriteContainer;
            }
            set
            {
                spriteContainer = value;
            }
        }
        public void SetInv(int key, ItemTile tile)
        {
            if (inventoryContainer.ContainsKey(key))
            {
                inventoryContainer[key] = tile;
            }
            else
            {
                inventoryContainer.Add(key, tile);
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
        public Dictionary<int, Tuple<int, Texture2D, Rectangle>> STATICSPRITESFunc
        {
            get
            {
                return STATICSPRITES;
            }
            set
            {
                this.STATICSPRITES = value;
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
        //Constructor
        public Game1()
        {
            this.IsMouseVisible = true;
            Content.RootDirectory = "Content";
            //Initialize all data structures (These are essentially Tiles/Grid Boxes/Static images). 
            spriteContainer = new Dictionary<int, Tuple<int, Texture2D, Rectangle>>();
            inventoryContainer = new Dictionary<int, ItemTile>();
            STATICSPRITES = new Dictionary<int, Tuple<int, Texture2D, Rectangle>>();
            currPlayerPosition = new Point();
            currMousePosition = new Point();
            graphics = new GraphicsDeviceManager(this);
        }
        //TODO: Determine if we should generated levels on initialization or LoadContent
        protected override void Initialize()
        {
            LoadGame();
            sButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - 50, 200);
            eButtonPos = new Vector2((GraphicsDevice.Viewport.Width / 2) - 50, 250);
            gameState = GameState.StartMenu;
            //Load all levels of the game
            grid = new GridLayout[10];
            for (int x = 0; x < 10; x++)
            {
                grid[x] = new GridLayout(this);
                //Generate grids based off layout matricies
                grid[x].generateGrid(x);
            }
            mouseState = Mouse.GetState();
            prevMouseState = mouseState;
            base.Initialize();
        }
        protected override void LoadContent()
        {
            sButton = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/start.jpg"));
            eButton = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/exit.jpg"));
            loadScreen = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/LoadScreen.jpg"));
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
            //Dynamic sprites that are annoying to handle within container element
            inventory = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/Inventory.png"));
            darkbg = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/DarkBG.png"));
            activePlayer = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/P1.png"));            
            //cursorDummyPassive = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/cursorSprite.png"));
            //cursorDummyActive = Texture2D.FromStream(GraphicsDevice, TitleContainer.OpenStream(@"Images/cursorDummyActive.png"));
            //////////////////////END PRE GAME LOADING
        }
        //TODO Do we need this for our game? :/
        protected override void UnloadContent()
        {
        }
        /* TODO
        2.) Implement Character Switching Method
        */
        protected override void Update(GameTime gameTime)
        {            
            if (gameState == GameState.Loading && !isLoading)
            {
                backgroundThread = new Thread(LoadGame);
                isLoading = true;
                backgroundThread.Start();
            }
            mouseState = Mouse.GetState();
            if(prevMouseState.LeftButton == ButtonState.Pressed && mouseState.LeftButton == ButtonState.Released)
            {
                MouseClicked(mouseState.X, mouseState.Y);
            }
            prevMouseState = mouseState;
            if(gameState == GameState.Playing && isLoading)
            {
                LoadGame();
                isLoading = false;
            }
            //Runs all methods relted to handling user input events.
            //ProcessInputFunctions has a chaining effect to all other detections
            physicsEngine.ProcessInputFunctions(Mouse.GetState(), Keyboard.GetState(), gameTime);
            base.Update(gameTime);
        }
        protected override void Draw(GameTime gameTime)
        {
            GraphicsDevice.Clear(Color.CornflowerBlue);
            spriteBatch.Begin();
            if(gameState == GameState.StartMenu)
            {
                spriteBatch.Draw(sButton, sButtonPos, Color.White);
                spriteBatch.Draw(eButton, eButtonPos, Color.White);
            }
            if(gameState == GameState.Loading)
            {
                spriteBatch.Draw(loadScreen, new Vector2((GraphicsDevice.Viewport.Width / 2) - (loadScreen.Width / 2), (GraphicsDevice.Viewport.Height / 2) - (loadScreen.Height / 2)), Color.YellowGreen);
            }
            if (gameState == GameState.Playing)
            {              
                //Load sprites
                drawActiveSprites();
                drawSprites();
                //Load mouse and player sprites
                spriteBatch.Draw(activePlayer, new Rectangle(currPlayerPosition.X, currPlayerPosition.Y - ActivePlayer.Height, ActivePlayer.Width, ActivePlayer.Height), Color.White);
                //spriteBatch.Draw(cursorDummyPassive, new Rectangle(currMousePosition.X, currMousePosition.Y, 15, 15), Color.White);
                //When inventory button is held down draw the inventory bag to the screen
                if (physicsEngine.IDOWNFunc)
                {
                    loadInventory();
                }
                /*
                If a player pushes Key.T this function draws the animation
                NOTE TDOWNFunc only returns true when all conditions are met within the physics engine.
                */
                if (physicsEngine.TDOWNFunc && !physicsEngine.EventStates[1])
                {
                    physicsEngine.checkTrajectoryEvent1();
                }
                if (physicsEngine.ACTIVELEVELFunc == 6)
                {
                    physicsEngine.dangerZoneSimulation();
                }
                if(physicsEngine.InterActiveProximity && !physicsEngine.EventStates[2])
                {
                    spriteBatch.Draw(interactionPrompt, new Rectangle(160, graphics.PreferredBackBufferHeight - (160 + interactionPrompt.Height), interactionPrompt.Width, interactionPrompt.Height),Color.White);
                }
                if(physicsEngine.ACTIVELEVELFunc == 8)
                {
                    physicsEngine.ohmCollisionDetection();
                    physicsEngine.scoreWires();
                }
                spriteBatch.Draw(pButton, new Vector2(graphics.PreferredBackBufferWidth - pButton.Width, 0), Color.White);
            }  
            if (gameState == GameState.Paused)
            {
                spriteBatch.Draw(rButton, rButtonPos, Color.White);
            }
            spriteBatch.End();
            base.Draw(gameTime);
        }
        /*
        Draw all tile based sprites to the screen, since our map is a static background with tile overlay, this method is relatively efficient, if switched to a full tile map
        this function becomes costly.
        TODO: Determine if its worth going with a full tile map.
        */
        public void drawSprites()
        {
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
            foreach (PlatForm tile in grid[physicsEngine.ACTIVELEVELFunc].PlatFormTile)
            {
                spriteBatch.Draw(tile.Image, tile.Box, Color.White);
            }
            foreach (LadderTile tile in grid[physicsEngine.ACTIVELEVELFunc].LadderTile)
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
        }
        /*
        If we currently have a item in our bag, we should draw that item to the user when requestion
        */
        public void loadInventory()
        {   
            spriteBatch.Draw(inventory, new Rectangle(graphics.PreferredBackBufferWidth - inventory.Width, graphics.PreferredBackBufferHeight - inventory.Height, inventory.Width, inventory.Height), Color.White);
            for (int x = 0; x < 10; x++)
            {
                if (inventoryContainer.ContainsKey(x))
                {
                    if (inventoryContainer[x].Collected && !inventoryContainer[x].Equipt && !inventoryContainer[x].Consumed)
                    {                       
                        spriteBatch.Draw(inventoryContainer[x].BagImage, new Rectangle(grid[0].GetMap.bagLocs[x].X, grid[0].GetMap.bagLocs[x].Y, inventoryContainer[x].BagImage.Width, inventoryContainer[x].BagImage.Height), Color.White);
                    }
                }
            }
        }
        /*
        Late in the game, generic draw seemed relevant
        */
        public void overloadedDraw(List<SpikeTile> temp)
        {
            for(int x = 0; x < temp.Count; x++)
            {                          
                spriteBatch.Draw(temp[x].Image, temp[x].Box, Color.White);
            }
        }
        /*
        Sort through our statically placed sprites in order to determine which sprites are appropriate to draw to the screen
        */
        public void drawActiveSprites()
        {
            for (int y = 0; y < STATICSSIZE; y++)
            {
                if (STATICSPRITES.ContainsKey(y) && STATICSPRITES[y].Item1 == -1 || STATICSPRITES.ContainsKey(y) && y > 9 && STATICSPRITES[y].Item1 > 0)
                {
                    spriteBatch.Draw(STATICSPRITES[y].Item2, STATICSPRITES[y].Item3, Color.White);
                }
                if (STATICSPRITES.ContainsKey(y) && STATICSPRITES[y].Item1 == -2 && physicsEngine.ACTIVELEVELFunc == 2 && !physicsEngine.EventStates[2])
                {
                    spriteBatch.Draw(STATICSPRITES[y].Item2, STATICSPRITES[y].Item3, Color.White);
                }
                if (STATICSPRITES.ContainsKey(y) && physicsEngine.ACTIVELEVELFunc == 8 && !(STATICSPRITES[y].Item1 == -2) && !(STATICSPRITES[y].Item1 == -5))
                {
                   
                    spriteBatch.Draw(STATICSPRITES[y].Item2, STATICSPRITES[y].Item3, Color.White);
                }
            }
            spriteBatch.Draw(STATICSPRITES[physicsEngine.ACTIVELEVELFunc + STATICOFFSET].Item2, STATICSPRITES[physicsEngine.ACTIVELEVELFunc + STATICOFFSET].Item3, Color.White);            
        }
        /*
            Performs the animations for action event 1 taking place on level 1, each level will have its own action event and require a method similar to this.
            TODO: Fix the animation for this event so that it looks realistic.
        */
        public void drawActionEvent1(GameTime gameTime)
        {
            if (inventoryContainer.ContainsKey(3) && inventoryContainer[3].Equipt)
            {
                for (int x = 0; x < physicsEngine.AniBoxes.Count; x++)
                {                  
                        spriteBatch.Draw(inventoryContainer[3].Image, physicsEngine.AniBoxes[x], Color.White);
                        lastEventTime -= gameTime.ElapsedGameTime.Milliseconds;                
                }           
                inventoryContainer[3].Equipt = false;
            }           
        }
        public void tempAniDraw()
        {             
            spriteBatch.Draw(darkbg, new Rectangle(0,0,graphics.PreferredBackBufferWidth, graphics.PreferredBackBufferHeight), Color.White);           
        }
    }
}