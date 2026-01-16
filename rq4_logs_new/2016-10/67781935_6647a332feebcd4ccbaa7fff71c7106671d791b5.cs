using System;
using System.Text;

namespace KataNetHack.Console.Renderer
{
    public class Renderer
    {
        private const int MAP_ROW_COUNT = 10;
        private const int MAP_COLUMN_COUNT = 10;
        private const char WALL_CHARACTER = '█';
        private const char PASSAGEWAY_CHARACTER = ' ';
        private const char EXIT_CHARACTER = '◌';

        private readonly IMap _map;

        public Action<string> WriteLine = System.Console.WriteLine;

        public Renderer(IMap map)
        {
            _map = map;
        }

        public void Render()
        {
            for(int row = 0;row < MAP_ROW_COUNT;row++)
            {
                var builder = new StringBuilder();

                for(var column = 0; column < MAP_COLUMN_COUNT; column++)
                {
                    if(_map.GetElementType(column, row) == ElementType.Wall)
                    {
                        builder.Append(WALL_CHARACTER);
                    }
                    else if(_map.GetElementType(column, row) == ElementType.Exit)
                    {
                        builder.Append(EXIT_CHARACTER);
                    }
                    else
                    {
                        builder.Append(PASSAGEWAY_CHARACTER);
                    }
                }

                WriteLine(builder.ToString());
            }
        }
    }
}