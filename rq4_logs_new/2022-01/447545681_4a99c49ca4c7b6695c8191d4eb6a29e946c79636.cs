using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using MonoGame.Extended.Content;
using MonoGame.Extended.Screens;
using MonoGame.Extended.Serialization;
using MonoGame.Extended.Sprites;
using MonoGame.Extended.Tiled;
using MonoGame.Extended.Tiled.Renderers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Text;
using Color = Microsoft.Xna.Framework.Color;


namespace lost_clothes_code
{
    public class niveau_1_1 : GameScreen
    {
        private Game1 _myGame; // pour récuperer le jeu en cours
        private TiledMap _tiledMap;
        private TiledMapRenderer _tiledMapRenderer;
        private const int HAUTEUR_PERSO = 45;
        private GraphicsDeviceManager _graphics;
        private SpriteBatch _spriteBatch;
        private Vector2 _persoPosition;
        private AnimatedSprite _perso;
        private int _vitessePerso;
        private int _vitesseMarche;     // nombre de pas par seconde pour l'_animationPerso
        private Stopwatch _stopWatchMarche;
        private Stopwatch _stopWatchSaut;
        private Stopwatch _stopWatchChute;
        private string _animationPerso;
        private int _dureeMaximaleSaut; // durée maximale de saut du perso en millisecondes
        private SpriteFont _font;
        private Vector2 _positionTexte;
        private AnimatedSprite _bulle;
        private Vector2 _bullePosition;
        private string _animationBulle;
        public niveau_1_1(Game1 game) : base(game)
        {
            Content.RootDirectory = "Content";
            _myGame = game;
        }

        public override void Initialize()
        {
            // TODO: Add your initialization logic here

            _persoPosition.X = 100;
            _persoPosition.Y = 375;
            _bullePosition.X = 400;
            _bullePosition.Y = 100;
            _vitessePerso = 200;
            _vitesseMarche = 2;
            _stopWatchMarche = new Stopwatch();
            _stopWatchMarche.Start();
            _stopWatchSaut = new Stopwatch();
            _stopWatchChute = new Stopwatch();
            _animationPerso = "d_idle";
            _positionTexte = new Vector2(0, 0);
            _animationBulle = "d_bulle_1";

            base.Initialize();
        }
        public override void LoadContent()
        {
            _tiledMap = Content.Load<TiledMap>("map_1_1");
            _tiledMapRenderer = new TiledMapRenderer(GraphicsDevice, _tiledMap);
            _spriteBatch = new SpriteBatch(GraphicsDevice);

            // TODO: use this.Content to load your game content here

            SpriteSheet spriteSheet = Content.Load<SpriteSheet>("chevalier_0.sf", new JsonContentLoader());
            SpriteSheet spriteSheete = Content.Load<SpriteSheet>("bulle_eau.sf", new JsonContentLoader());
            _perso = new AnimatedSprite(spriteSheet);
            _bulle = new AnimatedSprite(spriteSheete);
            _font = Content.Load<SpriteFont>("font");

        }

        public override void Update(GameTime gametime)
        {
            float deltaSeconds = (float)gametime.ElapsedGameTime.TotalSeconds;
            float walkSpeed = deltaSeconds * _vitessePerso;
            string sensHorizontal = "D";    // G = gauche, D = droite
            string sensVertical = "N";      // N = neutre, H = haut, B = bas

            KeyboardState keyboardState = Keyboard.GetState();

            if (keyboardState.IsKeyDown(Keys.Up))
            {
                _stopWatchSaut.Start();
                _stopWatchChute.Start();
                sensVertical = "H";
            }

            if (keyboardState.IsKeyDown(Keys.Left))
            {
                if (_stopWatchMarche.ElapsedMilliseconds >= 1000.0 / _vitesseMarche || _animationPerso.Substring(0, 1) == "d")
                {
                    if (sensVertical == "H")
                    {
                        if (_animationPerso == "g_jumping_2")
                            _animationPerso = "g_jumping_1";
                        else
                            _animationPerso = "g_jumping_2";
                    }
                    else
                    {
                        if (_animationPerso == "g_walking_2")
                            _animationPerso = "g_walking_1";
                        else
                            _animationPerso = "g_walking_2";
                    }
                    _stopWatchMarche.Restart();
                }
                _persoPosition.X -= walkSpeed;
            }
            else if (keyboardState.IsKeyDown(Keys.Right))
            {
                if (_stopWatchMarche.ElapsedMilliseconds >= 1000.0 / _vitesseMarche || _animationPerso.Substring(0, 1) == "g")
                {
                    if (sensVertical == "H")
                    {
                        if (_animationPerso == "d_jumping_2")
                            _animationPerso = "d_jumping_1";
                        else
                            _animationPerso = "d_jumping_2";
                    }
                    else
                    {
                        if (_animationPerso == "d_walking_2")
                            _animationPerso = "d_walking_1";
                        else
                            _animationPerso = "d_walking_2";
                    }
                    _stopWatchMarche.Restart();
                }
                _persoPosition.X += walkSpeed;
            }

            if (_stopWatchSaut.IsRunning)
                _persoPosition.Y -= walkSpeed;

            if (_persoPosition.Y < GraphicsDevice.Viewport.Height - HAUTEUR_PERSO / 2)
                _persoPosition.Y += _stopWatchChute.ElapsedMilliseconds * walkSpeed / 600;
            else
            {
                _persoPosition.Y = GraphicsDevice.Viewport.Height - HAUTEUR_PERSO / 2;
                _stopWatchSaut.Reset();
                _stopWatchChute.Reset();
            }

            if (_bullePosition.X > _persoPosition.X)
            {
                _bullePosition.X = _bullePosition.X - 1;
                _animationBulle = "g_bulle_1";
            }
            else
            {
                _bullePosition.X = _bullePosition.X + 1;
                _animationBulle = "d_bulle_1";
            }

            if (_bullePosition.Y > _persoPosition.Y + 15)
            {
                _bullePosition.Y = _bullePosition.Y - 1;
            }
            else
            {
                _bullePosition.Y = _bullePosition.Y + 1;
            }

            _perso.Play(_animationPerso);
            _perso.Update(deltaSeconds);
            _bulle.Play(_animationBulle);
            _bulle.Update(gametime);
            _tiledMapRenderer.Update(gametime);
        }

        public override void Draw(GameTime gametime)
        {
            _myGame.GraphicsDevice.Clear(Color.CornflowerBlue);


            _myGame.SpriteBatch.Begin();
            _tiledMapRenderer.Draw();
            _spriteBatch.Begin();
            _spriteBatch.Draw(_perso, _persoPosition);
            _spriteBatch.Draw(_bulle, _bullePosition);
            _spriteBatch.End();
            _myGame.SpriteBatch.End();
        }

    }
}