using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using MonoGame.Extended.Content;
using MonoGame.Extended.Screens;
using MonoGame.Extended.Screens.Transitions;
using MonoGame.Extended.Serialization;
using MonoGame.Extended.Sprites;
using System.Diagnostics;

namespace lost_clothes_code
{
    public class Game1 : Game
    {
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
        private readonly ScreenManager _screenManager;
        public SpriteBatch SpriteBatch { get => _spriteBatch; set => _spriteBatch = value; }

        public Game1()
        {
            _graphics = new GraphicsDeviceManager(this);
            Content.RootDirectory = "Content";
            IsMouseVisible = true;
            _screenManager = new ScreenManager();
            Components.Add(_screenManager);
        }

        protected override void Initialize()
        {
            // TODO: Add your initialization logic here

            _persoPosition.X = 100;
            _persoPosition.Y = 400;
            _vitessePerso = 200;
            _vitesseMarche = 2;
            _stopWatchMarche = new Stopwatch();
            _stopWatchMarche.Start();
            _stopWatchSaut = new Stopwatch();
            _stopWatchChute = new Stopwatch();
            _animationPerso = "d_idle";
            _positionTexte = new Vector2(0, 0);

            base.Initialize();
        }

        protected override void LoadContent()
        {
            _spriteBatch = new SpriteBatch(GraphicsDevice);

            // TODO: use this.Content to load your game content here

            SpriteSheet spriteSheet = Content.Load<SpriteSheet>("chevalier_0.sf", new JsonContentLoader());
            _perso = new AnimatedSprite(spriteSheet);
            _font = Content.Load<SpriteFont>("font");
        }

        protected override void Update(GameTime gameTime)
        {
            if (GamePad.GetState(PlayerIndex.One).Buttons.Back == ButtonState.Pressed || Keyboard.GetState().IsKeyDown(Keys.Escape))
                Exit();

            // TODO: Add your update logic here

            float deltaSeconds = (float)gameTime.ElapsedGameTime.TotalSeconds;
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

            keyboardState = Keyboard.GetState();
            if (keyboardState.IsKeyDown(Keys.Left))
            {
                LoadScreen1();
            }

            _perso.Play(_animationPerso);
            _perso.Update(deltaSeconds);

            base.Update(gameTime);
        }

        protected override void Draw(GameTime gameTime)
        {
            GraphicsDevice.Clear(Color.CornflowerBlue);

            // TODO: Add your drawing code here

            _spriteBatch.Begin();
            _spriteBatch.Draw(_perso, _persoPosition);
            _spriteBatch.DrawString(_font, $"_stopWatchMarche : {_stopWatchMarche.ElapsedMilliseconds} " +
                $"_stopWatchSaut {_stopWatchSaut.ElapsedMilliseconds} " +
                $"_stopWatchChute {_stopWatchChute.ElapsedMilliseconds}" +
                $"_persoPosition.y {_persoPosition.Y}", _positionTexte, Color.White);
            _spriteBatch.End();

            base.Draw(gameTime);
        }
        private void LoadScreen1()
        {
            _screenManager.LoadScreen(new niveau_1_0(this), new FadeTransition(GraphicsDevice, Color.Black));
        }
    }

}