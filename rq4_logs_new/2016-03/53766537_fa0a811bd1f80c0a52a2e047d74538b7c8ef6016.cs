using System.IO;
using System.Linq;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;

namespace TexturePackerLoader
{
    public abstract class BaseSpriteSheetLoader
    {
        private readonly ContentManager _contentManager;

        protected BaseSpriteSheetLoader(ContentManager contentManager)
        {
            _contentManager = contentManager;
        }

        public SpriteSheet MultiLoad(string imageResourceFormat, int numSheets)
        {
            var result = new SpriteSheet();

            for (var i = 0; i < numSheets; i++)
            {
                var imageResource = string.Format(imageResourceFormat, i);

                var tmp = Load(imageResource);
                result.Add(tmp);
            }

            return result;
        }

        public SpriteSheet Load(string imageResource)
        {
            var texture = _contentManager.Load<Texture2D>(imageResource);

            var dataFile = Path.Combine(_contentManager.RootDirectory, Path.ChangeExtension(imageResource, "txt"));

            var dataFileLines = ReadDataFile(dataFile);

            var sheet = new SpriteSheet();

            foreach (
                var cols in
                    from row in dataFileLines
                    where !string.IsNullOrEmpty(row) && !row.StartsWith("#")
                    select row.Split(';'))
            {
                if (cols.Length != 10)
                {
                    throw new InvalidDataException("Incorrect format data in spritesheet data file");
                }

                var isRotated = int.Parse(cols[1]) == 1;

                var name = cols[0];

                var rectangle = new Rectangle(
                    int.Parse(cols[2]),
                    int.Parse(cols[3]),
                    int.Parse(cols[4]),
                    int.Parse(cols[5]));

                var size = new Vector2(
                    int.Parse(cols[6]),
                    int.Parse(cols[7]));

                var pivotPoint = new Vector2(
                    float.Parse(cols[8]),
                    float.Parse(cols[9]));

                var sprite = new SpriteFrame(texture, rectangle, size, pivotPoint, isRotated);

                sheet.Add(name, sprite);
            }

            return sheet;
        }

        public abstract string[] ReadDataFile(string filePath);
    }
}