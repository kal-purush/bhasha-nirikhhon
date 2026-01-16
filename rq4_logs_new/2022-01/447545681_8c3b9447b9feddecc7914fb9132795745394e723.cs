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
    public class niveau1_1 : GameScreen
    {
        private Game1 _myGame; // pour récuperer le jeu en cours

        private TiledMap _tiledMap; // pour les collisions et generer la map
        private TiledMapRenderer _tiledMapRenderer;
        private TiledMapTileLayer _mapLayer;

        private const int HAUTEUR_PERSO = 45;
        private const int LARGEUR_PERSO = 27;

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

        public niveau1_1(Game1 game) : base(game)
        {
            Content.RootDirectory = "Content";
            _myGame = game;
        }

        public override void Initialize()
        {
            // TODO: Add your initialization logic here

            _persoPosition.X = 100;
            _persoPosition.Y = 300;
            _vitessePerso = 200;
            _vitesseMarche = 2;
            _stopWatchMarche = new Stopwatch();
            _stopWatchMarche.Start();
            _stopWatchSaut = new Stopwatch();
            _stopWatchChute = new Stopwatch();
            _animationPerso = "d_idle";
            base.Initialize();
        }
        public override void LoadContent()
        {
            _tiledMap = Content.Load<TiledMap>("Maps/map_1_1");
            _mapLayer = _tiledMap.GetLayer<TiledMapTileLayer>("briques");
            _tiledMapRenderer = new TiledMapRenderer(GraphicsDevice, _tiledMap);
            _spriteBatch = new SpriteBatch(GraphicsDevice);

            // TODO: use this.Content to load your game content here

            SpriteSheet spriteSheet = Content.Load<SpriteSheet>("chevalier_0.sf", new JsonContentLoader());
            _perso = new AnimatedSprite(spriteSheet);

        }

        public override void Update(GameTime gametime)
        {
            float deltaSeconds = (float)gametime.ElapsedGameTime.TotalSeconds;
            float walkSpeed = deltaSeconds * _vitessePerso;
            string sensHorizontal = "D";    // G = gauche, D = droite
            string sensVertical = "N";      // N = neutre, H = haut, B = bas
            ushort txUp = (ushort)(_persoPosition.X / _tiledMap.TileWidth);
            ushort tyUp = (ushort)((_persoPosition.Y + HAUTEUR_PERSO / 2) / _tiledMap.TileHeight - 1); // tuile au-dessus
            ushort txLeft = (ushort)((_persoPosition.X + LARGEUR_PERSO) / _tiledMap.TileWidth - 1); // tuile à gauche
            ushort tyLeft = (ushort)(_persoPosition.Y / _tiledMap.TileHeight);
            ushort txRight = (ushort)((_persoPosition.X - LARGEUR_PERSO) / _tiledMap.TileWidth + 1); // tuile à droite
            ushort tyRight = (ushort)((_persoPosition.Y) / _tiledMap.TileHeight);
            ushort txDown = (ushort)(_persoPosition.X / _tiledMap.TileWidth);
            ushort tyDown = (ushort)((_persoPosition.Y - HAUTEUR_PERSO / 2) / _tiledMap.TileHeight + 1); // tuile eu-dessous


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

                if (!IsCollision(txLeft, tyLeft))
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

                if (!IsCollision(txRight, tyRight))
                    _persoPosition.X += walkSpeed;
            }

            if (_stopWatchSaut.IsRunning && !IsCollision(txUp, tyUp))
                _persoPosition.Y -= walkSpeed;
            else
                _stopWatchSaut.Reset();

            if (!IsCollision(txDown, tyDown))
                _persoPosition.Y += _stopWatchChute.ElapsedMilliseconds * walkSpeed / 600;
            else
            {
                _stopWatchSaut.Reset();
                _stopWatchChute.Reset();
            }
            if (keyboardState.IsKeyDown(Keys.Down))
            {
                _myGame.LoadScreen1_2();
            }

            _perso.Play(_animationPerso);
            _perso.Update(deltaSeconds);

            _tiledMapRenderer.Update(gametime);

        }

        public override void Draw(GameTime gametime)
        {
            _myGame.GraphicsDevice.Clear(Color.CornflowerBlue);
            _myGame.SpriteBatch.Begin();
            _tiledMapRenderer.Draw();
            _spriteBatch.Begin();
            _spriteBatch.Draw(_perso, _persoPosition);
            _spriteBatch.End();
            _myGame.SpriteBatch.End();
        }

        private bool IsCollision(ushort x, ushort y)
        {
            if (_mapLayer.GetTile(x, y).GlobalIdentifier > 0 && _mapLayer.GetTile(x, y).GlobalIdentifier < 43)
                return true;

            return false;
        }
        
    }
}