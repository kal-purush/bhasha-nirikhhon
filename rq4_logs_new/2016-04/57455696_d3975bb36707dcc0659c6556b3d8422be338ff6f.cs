using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;


namespace Game2
{
    public class Stone
    {
        int typ;
        int horizontal = 0;
        Vector2[] positions = new Vector2[4];
        bool stonepresent = false;
        Vector2 rechtsOben = new Vector2(20, -20);
        Vector2 linksOben = new Vector2(-20, -20);
        Vector2 linksUnten = new Vector2(-20, 20);
        Vector2 rechtsUnten = new Vector2(20, 20);

        Vector2 HundertNull = new Vector2(100, 0);
        Vector2 HundertzwanzigNull = new Vector2(120, 0);
        Vector2 HundertZwanzig = new Vector2(100, 20);
        Vector2 HundertzwanzigZwanzig = new Vector2(120, 20);


        public bool Stonepresent
        {
            get
            {
                return stonepresent;
            }
            set
            {
                stonepresent = value;
            }
        }

        public Stone()
        {
            Random random = new Random();
            this.typ = random.Next(0, 7);
            switch (typ)
            {
                case 0:
                    positions[0] = HundertNull;
                    positions[1] = HundertzwanzigNull;
                    positions[2] = HundertzwanzigZwanzig;
                    positions[3] = new Vector2(140, 20);
                    this.stonepresent = true;
                    break;

                case 1:
                    positions[0] = new Vector2(80, 20);
                    positions[1] = HundertZwanzig;
                    positions[2] = HundertNull;
                    positions[3] = HundertzwanzigNull;
                    this.stonepresent = true;
                    break;

                case 2:
                    positions[0] = HundertNull;
                    positions[1] = HundertZwanzig;
                    positions[2] = new Vector2(100, 40);
                    positions[3] = new Vector2(100, 60);
                    this.stonepresent = true;
                    break;

                case 3:
                    positions[0] = HundertNull;
                    positions[1] = HundertzwanzigNull;
                    positions[2] = HundertZwanzig;
                    positions[3] = HundertzwanzigZwanzig;
                    this.stonepresent = true;
                    break;

                case 4:
                    positions[0] = HundertNull;
                    positions[1] = HundertzwanzigNull;
                    positions[2] = HundertzwanzigZwanzig;
                    positions[3] = new Vector2(120, 40);
                    this.stonepresent = true;
                    break;

                case 5:
                    positions[0] = HundertzwanzigNull;
                    positions[1] = HundertNull;
                    positions[2] = HundertZwanzig;
                    positions[3] = new Vector2(100, 40);
                    this.stonepresent = true;
                    break;

                case 6:
                    positions[0] = HundertZwanzig;
                    positions[1] = HundertzwanzigZwanzig;
                    positions[2] = HundertzwanzigNull;
                    positions[3] = new Vector2(140, 20);
                    this.stonepresent = true;
                    break;

            }
        }

        public Vector2 get(int i)
        {
            return positions[i];
        }

        public void set(int i, Vector2 vec)
        {
            positions[i] += vec;
        }

        public int XCoord(int i)
        {
            return (int)(this.get(i).X / 20);
        }


        public int YCoord(int i)
        {
            return (int)(this.get(i).Y / 20);
        } 

        public void rotate()
        {
            if ((typ == 0) & ((positions[0].X > 10)) & (positions[0].X < 180))
            {
                if (horizontal == 0)
                {
                    positions[0] += rechtsOben;
                    positions[2] += linksOben;
                    positions[3] += new Vector2(-40, 0);
                    horizontal = 1;
                }
                else
                {
                    positions[0] += linksUnten;
                    positions[2] += rechtsUnten;
                    positions[3] += new Vector2(40, 0);
                    horizontal = 0;
                }
            }
            else if ((typ == 1) & ((positions[0].X > 10)) & (positions[0].X < 180))
            {
                if (horizontal == 0)
                {
                    positions[0] += rechtsOben;
                    positions[2] += rechtsUnten;
                    positions[3] += new Vector2(0, 40);
                    horizontal = 1;
                }
                else
                {
                    positions[0] += linksUnten;
                    positions[2] += linksOben;
                    positions[3] += new Vector2(0, -40);
                    horizontal = 0;
                }
            }
            else if ((typ == 2) & ((positions[0].X > 10)) & (positions[0].X < 160))
            {
                if (horizontal == 0)
                {
                    positions[0] += linksUnten;
                    positions[2] += rechtsOben;
                    positions[3] += 2 * rechtsOben;
                    horizontal = 1;
                }
                else
                {
                    positions[0] += rechtsOben;
                    positions[2] += linksUnten;
                    positions[3] += 2 * linksUnten;
                    horizontal = 0;
                }
            }

            else if ((typ == 4) & ((positions[1].X > 10)) & (positions[1].X < 180))
            {
                if (horizontal == 0)
                {
                    positions[0] += rechtsOben;
                    positions[2] += linksOben;
                    positions[3] += 2 * linksOben;
                    horizontal = 1;
                }
                else if (horizontal == 1)
                {
                    positions[0] += rechtsUnten;
                    positions[2] += rechtsOben;
                    positions[3] += 2 * rechtsOben;
                    horizontal = 2;
                }
                else if (horizontal == 2)
                {
                    positions[0] += linksUnten;
                    positions[2] += rechtsUnten;
                    positions[3] += 2 * rechtsUnten;
                    horizontal = 3;
                }
                else if (horizontal == 3)
                {
                    positions[0] += linksOben;
                    positions[2] += linksUnten;
                    positions[3] += 2 * linksUnten;
                    horizontal = 0;
                }
            }

            else if ((typ == 5) & ((positions[1].X > 10)) & (positions[1].X < 180))
            {
                if (horizontal == 0)
                {
                    positions[0] += linksUnten;
                    positions[2] += linksOben;
                    positions[3] += 2 * linksOben;
                    horizontal = 1;
                }
                else if (horizontal == 1)
                {
                    positions[0] += linksOben;
                    positions[2] += rechtsOben;
                    positions[3] += 2 * rechtsOben;
                    horizontal = 2;
                }
                else if (horizontal == 2)
                {
                    positions[0] += rechtsOben;
                    positions[2] += rechtsUnten;
                    positions[3] += 2 * rechtsUnten;
                    horizontal = 3;
                }
                else if (horizontal == 3)
                {
                    positions[0] += rechtsUnten;
                    positions[2] += linksUnten;
                    positions[3] += 2 * linksUnten;
                    horizontal = 0;
                }
            }

            else if ((typ == 6) & ((positions[1].X > 10)) & (positions[1].X < 180))
            {
                if (horizontal == 0)
                {
                    positions[0] += rechtsOben;
                    positions[2] += rechtsUnten;
                    positions[3] += linksUnten;
                    horizontal = 1;
                }
                else if (horizontal == 1)
                {
                    positions[0] += rechtsUnten;
                    positions[2] += linksUnten;
                    positions[3] += linksOben;
                    horizontal = 2;
                }
                else if (horizontal == 2)
                {
                    positions[0] += linksUnten;
                    positions[2] += linksOben;
                    positions[3] += rechtsOben;
                    horizontal = 3;
                }
                else if (horizontal == 3)
                {
                    positions[0] += linksOben;
                    positions[2] += rechtsOben;
                    positions[3] += rechtsUnten;
                    horizontal = 0;
                }
            }


        }
    }


    public class Game1 : Game
    {
        GraphicsDeviceManager graphics;
        SpriteBatch spriteBatch;
        KeyboardState oldState, newState;
        Texture2D stone;
        int[,] spielfeld = new int[10, 20];
        bool keypressed = false;
        Stone newStone;
        int[,] setStones = new int[10,20];
        float rotTimer = 0f;

        public Game1()
        {
            graphics = new GraphicsDeviceManager(this);
            Content.RootDirectory = "Content";
            graphics.PreferredBackBufferWidth = 200;
            graphics.PreferredBackBufferHeight = 400;
        }

        protected override void Initialize()
        {
            base.Initialize();
            oldState = Keyboard.GetState();
            newStone = new Stone();
            for (int x = 0; x < 10; x++)
            {
                for (int y = 0; y < 20; y++)
                {
                    setStones[x, y] = 0;
                }
            }
        }

        protected override void LoadContent()
        {
            spriteBatch = new SpriteBatch(GraphicsDevice);
            stone = Content.Load<Texture2D>("Stone");
        }

        protected override void UnloadContent()
        {
            // TODO: Unload any non ContentManager content here
        }

        protected override void Update(GameTime gameTime)
        {
            if (!newStone.Stonepresent)
            {
                newStone = new Stone();
            }
            if ((gameTime.TotalGameTime.Milliseconds % 400 == 0))
            {
                for (int i = 0; i < 4; i++)
                    newStone.set(i, new Vector2(0, 20));
            }
            if ((gameTime.TotalGameTime.Milliseconds % 50 == 0))
            {
                keypressed = false;
            }
            int hilfe = 0;
            for (int y = 0; y < 20; y++)
            {
                for (int x = 0; x < 10; x++)
                {
                    if (setStones[x, y] == 1)
                        hilfe += 1;
                }
                if (hilfe == 10)
                    for (int y2 = y; y2 > 0; y2--)
                    {
                        for (int x = 0; x < 10; x++)
                        {
                            setStones[x, y2] = setStones[x, y2 - 1];
                        }
                        
                    }
                hilfe = 0;
            }
            for (int i = 0; i < 4; i++)
                if (newStone.get(i).Y >= 380 || setStones[newStone.XCoord(i), newStone.YCoord(i)+1] == 1)
                {
                    newStone.Stonepresent = false;
                    for (int j = 0; j < 4; j++)
                        setStones[newStone.XCoord(j), newStone.YCoord(j)] = 1;
                }

            rotTimer += gameTime.ElapsedGameTime.Milliseconds;
            
            UpdateInput();
            base.Update(gameTime);
        }

        protected override void Draw(GameTime gameTime)
        {
            GraphicsDevice.Clear(Color.CornflowerBlue);
            spriteBatch.Begin(SpriteSortMode.Deferred, BlendState.AlphaBlend);
            for (int i = 0; i < 4; i++)
                spriteBatch.Draw(stone, newStone.get(i), Color.White);
            for (int x = 0; x < 10; x++)
            {
                for (int y = 0; y < 20; y++)
                {
                    if (setStones[x,y] == 1)
                        spriteBatch.Draw(stone, new Vector2(x*20, y*20), Color.White);
                }
            }
            spriteBatch.End();
            base.Draw(gameTime);
        }
        private void UpdateInput()
        {
            newState = Keyboard.GetState();

            if (newState.IsKeyDown(Keys.Space))
            {
                Exit();
            }

            if (keypressed == false)
            {
                if (newState.IsKeyDown(Keys.Left))
                {
                    int k = 0;
                    for (int i = 0; i < 4; i++)
                    {
                        if (newStone.get(i).X > 10 && (setStones[newStone.XCoord(i)-1, newStone.YCoord(i)]!= 1))
                            k += 1;

                    }
                    if (k == 4)
                    {
                        for (int i = 0; i < 4; i++)
                        {
                            newStone.set(i, new Vector2(-20, 0));
                        }
                    }

                }
                else if (newState.IsKeyDown(Keys.Right))
                {

                        int k = 0;
                        for (int i = 0; i < 4; i++)
                        {

                            if (newStone.get(i).X < 180 && (setStones[newStone.XCoord(i)+1, newStone.YCoord(i)] != 1))
                                k += 1;

                        }
                        if (k == 4)
                        {
                            for (int i = 0; i < 4; i++)
                            {
                                newStone.set(i, new Vector2(20, 0));
                            }
                        }
                    }
                else if (newState.IsKeyDown(Keys.Down))
                {
                    for (int i = 0; i < 4; i++)
                        newStone.set(i, new Vector2(0, 20));
                }
                else if (newState.IsKeyDown(Keys.Up) & (rotTimer > 300))
                {
                    newStone.rotate();
                    rotTimer = 0f;
                }
            keypressed = true;
            }
        }
    }
}