using Engine.Components;
using Engine.Game;
using Engine.Infrastructure;
using OpenTK.Mathematics;

namespace CityGeneration
{
    class Program
    {
        public static void Main()
        {
            var game = new Game();
            SetupGame(game);
            game.Run();
        }

        private static void SetupGame(Game game)
        {
            game.Light.Position = new Vector3(0, 10, 0);
            CreateOperator(game);

            int planeSize = 24;

            var planeGo = game.Engine.CreateGameObject();
            planeGo.Add(new RenderComponent(game)
            {
                Material =
                {
                    Ambient = new Vector3(0.0f, 0.0f, 0.0f),
                    Diffuse = new Vector3(0.1f, 0.35f, 0.1f),
                    Specular = new Vector3(0.45f, 0.55f, 0.45f),
                    Shininess = 0.25f
                },
            });
            planeGo.Position = new Vector3(0);
            planeGo.Scale = new Vector3(planeSize);


            var random = new Random();
            int magic = 4;
            float scale = magic;

            var step = planeSize / magic;
            var leftZ = -planeSize / 2 + step / 2;

            for (int row = 0; row < magic; row++)
            {
                var leftX = -planeSize / 2 + step / 2;

                for (int col = 0; col < magic; col++)
                {
                    CreateBox(game,
                        new Vector3(scale),
                        new Vector3(0, random.Next(90), 0),
                        new Vector3(leftX, (float)scale / 2, leftZ));
                    leftX += step;
                }

                leftZ += step;
                scale /= 2;
            }
        }

        private static void CreateOperator(Game game)
        {
            var go = game.Engine.CreateGameObject();
            go.Add(new CameraControlComponent(game));
            go.Position = Vector3.UnitY * 5;
        }

        private static GameObject CreateBox(Game game, Vector3 scale, Vector3 rotation, Vector3 position)
        {

            var boxGo = game.Engine.CreateGameObject();
            boxGo.Add(new RenderComponent(game)
            {
                Vertices = GetCubeVertices(),
            });
            boxGo.Scale = scale;
            boxGo.Rotation = rotation;
            boxGo.Position = position;
            return boxGo;
        }

        private static float[] GetCubeVertices() => new[]
        {
            -0.5f, -0.5f, -0.5f,  0.0f,  0.0f, -1.0f,
            0.5f, -0.5f, -0.5f,  0.0f,  0.0f, -1.0f,
            0.5f,  0.5f, -0.5f,  0.0f,  0.0f, -1.0f,
            0.5f,  0.5f, -0.5f,  0.0f,  0.0f, -1.0f,
            -0.5f,  0.5f, -0.5f,  0.0f,  0.0f, -1.0f,
            -0.5f, -0.5f, -0.5f,  0.0f,  0.0f, -1.0f,

            -0.5f, -0.5f,  0.5f,  0.0f,  0.0f, 1.0f,
            0.5f, -0.5f,  0.5f,  0.0f,  0.0f, 1.0f,
            0.5f,  0.5f,  0.5f,  0.0f,  0.0f, 1.0f,
            0.5f,  0.5f,  0.5f,  0.0f,  0.0f, 1.0f,
            -0.5f,  0.5f,  0.5f,  0.0f,  0.0f, 1.0f,
            -0.5f, -0.5f,  0.5f,  0.0f,  0.0f, 1.0f,

            -0.5f,  0.5f,  0.5f, -1.0f,  0.0f,  0.0f,
            -0.5f,  0.5f, -0.5f, -1.0f,  0.0f,  0.0f,
            -0.5f, -0.5f, -0.5f, -1.0f,  0.0f,  0.0f,
            -0.5f, -0.5f, -0.5f, -1.0f,  0.0f,  0.0f,
            -0.5f, -0.5f,  0.5f, -1.0f,  0.0f,  0.0f,
            -0.5f,  0.5f,  0.5f, -1.0f,  0.0f,  0.0f,

            0.5f,  0.5f,  0.5f,  1.0f,  0.0f,  0.0f,
            0.5f,  0.5f, -0.5f,  1.0f,  0.0f,  0.0f,
            0.5f, -0.5f, -0.5f,  1.0f,  0.0f,  0.0f,
            0.5f, -0.5f, -0.5f,  1.0f,  0.0f,  0.0f,
            0.5f, -0.5f,  0.5f,  1.0f,  0.0f,  0.0f,
            0.5f,  0.5f,  0.5f,  1.0f,  0.0f,  0.0f,

            -0.5f, -0.5f, -0.5f,  0.0f, -1.0f,  0.0f,
            0.5f, -0.5f, -0.5f,  0.0f, -1.0f,  0.0f,
            0.5f, -0.5f,  0.5f,  0.0f, -1.0f,  0.0f,
            0.5f, -0.5f,  0.5f,  0.0f, -1.0f,  0.0f,
            -0.5f, -0.5f,  0.5f,  0.0f, -1.0f,  0.0f,
            -0.5f, -0.5f, -0.5f,  0.0f, -1.0f,  0.0f,

            -0.5f,  0.5f, -0.5f,  0.0f,  1.0f,  0.0f,
            0.5f,  0.5f, -0.5f,  0.0f,  1.0f,  0.0f,
            0.5f,  0.5f,  0.5f,  0.0f,  1.0f,  0.0f,
            0.5f,  0.5f,  0.5f,  0.0f,  1.0f,  0.0f,
            -0.5f,  0.5f,  0.5f,  0.0f,  1.0f,  0.0f,
            -0.5f,  0.5f, -0.5f,  0.0f,  1.0f,  0.0f
        };
    }
}