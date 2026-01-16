
using SFML.Graphics;
using SFML.Window;
using System.Drawing;
using System;

namespace ProjectSheep{
    class Game{
        static Schaf schaf1;
        static Schaf schaf2;
        public static Player Player { get; private set; }
        public static Map map { get; private set; }
        static GameTime gTime;
        static View view;

            static void Main(string[] args){
                RenderWindow win = new RenderWindow(new VideoMode(900,900), "ProjectSheep",Styles.Default|Styles.Fullscreen);
                win.Closed += (sender, e) => { ((RenderWindow)sender).Close(); };
                Initialize();

                while (win.IsOpen()){
                    Update();
                    Draw(win);
                    win.DispatchEvents();
                }
            }

       public static void Initialize(){
            gTime = new GameTime();
            map = new Map(new System.Drawing.Bitmap("Bilder/map.bmp"));
       //     Player = new Player(new Vector2f(map.TileSize + 500, map.TileSize + 900));
         //   Schaf schaf1 = new Schaf("", new Vector2f(900,5));
          //  Schaf schaf2 = new Schaf("", new Vector2f(100,5));
            view = new View();
       }

        static void Draw(RenderWindow window){
            window.Clear(new SFML.Graphics.Color(100, 100, 100));
            map.Draw(window);
      //      schaf1.Draw(window);
          //  schaf2.Draw(window);
        //    Player.Draw(window);
            window.Display();
            window.SetView(view);
        }

        static void Update(){
            gTime.Update();
        //    schaf1.Update(gTime);
          //  schaf2.Update(gTime);
            //Player.Update(gTime);
        }
    }
}