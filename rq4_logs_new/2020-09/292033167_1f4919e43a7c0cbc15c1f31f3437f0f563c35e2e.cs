using System.IO;
using System;

namespace Game
{
    public class Program{
        public class Entity{
            public string Name;
            public bool CanGoFrom;
            public char Image;
            public Entity(string name = "Unknown",bool CanGoFrom = true, char Image = '?'){
                this.Name=name;
                this.CanGoFrom = CanGoFrom;
                this.Image = Image;
            }
        }
        public class Wall: Entity{
            public Wall():base("Wall",false,'H'){}
        }
        public class Earth: Entity{
            public Earth():base("Earth",true,' '){}
        }
        public class NPC: Entity{
            public NPC(string name = "Unknown"):base("Unknown NPC",true,'8'){}
        }
        public class Player: Entity{
            public Player(int x,int y):base("Player",true,'&'){
                X=x;
                Y=y;
            }
            public void MoveRelative(int x ,int y){
                X += x;
                Y += y;
            }
            public int X;
            public int Y;
        }
        static public void DrawHorizontalBorder(int weight=10){
            for (int i = 0; i < weight+2; i++)
            {
                Console.Write('#');
            }
            Console.WriteLine();
        }
        class MapCheker{
            public Map _Map { get;private set; }
            public bool CanMove(int x,int y){
                try
                {
                    return _Map[x,y].CanGoFrom;    
                }
                catch (System.Exception)
                {
                    return false;
                }
            }
            public MapCheker(Map map){
                _Map = map;
            }
        }
        class EntityCreater{
            Map _Map;
            public EntityCreater(Map map){
                _Map = map;
            }
            public Entity Create(Entity entity,int x, int y){
                _Map[x,y] = entity;
                return entity;
            }
        }
        class Map
        {
            Entity [,] map;
            public int Width  { get; }
            public int Height { get; }
            public Map(int width,int height){
                Width = width;
                Height = height; 
                map = new Entity[width,height];
            } 
            public Entity this[int row,int collon]
            {
                get {  
                    return map[row,collon];
                }
                set {  
                    map[row,collon] = value;
                }
            }
        }
        public static void Main(){
            int weight= 10;
            int height= 10;
            int CountWall= 10;
            Map map = new Map(weight,height);
            MapCheker mapCheker = new MapCheker(map);
            EntityCreater entityCreater = new EntityCreater(map);

            //Create Player();
            Player player = new Player(1,0);

            //Create Earth();
            for (int y = 0; y < height; y++)
            {
                for (int x = 0; x < weight; x++)
                {
                    entityCreater.Create(new Earth(),x,y);   
                }
            }
            //Create walls
            for (int i = 0; i < CountWall; i++)
            {
                Random random = new Random();
                map[random.Next(0,9),random.Next(0,9)] = new Wall();
            }
            

            redraw:;
            Console.Clear();
            DrawHorizontalBorder();
            for (int i = 0; i < height; i++)
            {
                Console.Write('#');
                for (int j = 0; j < weight; j++)
                {
                    if(i == player.Y && j == player.X){
                        Console.Write(player.Image);    
                    }else{
                        Console.Write(map[j,i].Image);
                    }
                    
                }
                Console.WriteLine("#");
            }
            DrawHorizontalBorder();
            char key= Console.ReadKey(true).KeyChar;
            int dir_hor = key == 'a' ? -1:key == 'd'? 1:0;// if key = 'a' then return -1 or if key = 'd' return 0 else return 0
            int dir_ver = key == 'w' ? -1:key == 's'? 1:0;//Same how stroke up but for verticales
            try
            {
                Entity last_entity =  map[player.X+dir_hor,player.Y+dir_ver];//This Entity what you tryed contact last time    
                if(last_entity.CanGoFrom){
                    player.MoveRelative(dir_hor,dir_ver);
                }
            }
            catch (System.Exception)
            {
            }
            goto redraw;
        }
    }
}