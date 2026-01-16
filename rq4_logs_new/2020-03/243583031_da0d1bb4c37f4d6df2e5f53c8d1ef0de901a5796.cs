using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;

namespace GigglyLib.ProcGen
{
    internal class Circle
    {
        public Circle(Vector2 pos, float radius, float spawnDir)
        {
            P = pos;
            R = radius;
            SpawnDir = spawnDir;
        }
        public Circle(float x, float y, float radius, float spawnDir)
        {
            P = new Vector2(x, y);
            R = radius;
            SpawnDir = spawnDir;
        }
        public Vector2 P;
        public float R;
        public float SpawnDir;
    }

    public class MetaballGenerator
    {
        List<Circle> _circles = new List<Circle>();
        float _rStart;
        float _rDecrease;
        Random _rand;

        public MetaballGenerator(float startingR, float rDecrease, int seed)
        {
            _rStart = startingR;
            _rDecrease = rDecrease;
            _rand = new Random(seed);
        }

        public List<(int x, int y)> Generate()
        {
            Console.WriteLine("Metaball generator Generate() method was called");
            _circles = new List<Circle> {
                new Circle(0, 0, _rStart, 0f),
                new Circle(0, 0, _rStart, 0f),
                new Circle(0, 0, _rStart, 0f)
            };

            AddCircles(_circles);
            return GetOverlappingTiles();
        }

        private void AddCircles(List<Circle> roots)
        {
            List<Circle> open = new List<Circle>(roots);

            while (open.Count > 0)
            {
                Circle circle = CreateCircleChild(open[0]);
                _circles.Add(circle);

                open.RemoveAt(0);
                if (_rand.NextDouble() <= 0.95f)
                    open.Add(circle);
            }
        }

        private Circle CreateCircleChild(Circle parent)
        {
            float radius = parent.R * _rDecrease;
            float angle = 0f;
            if (parent.SpawnDir == 0f)
                angle = (float)_rand.NextDouble() * 6.282f;
            else
            {
                angle = parent.SpawnDir + (float)_rand.NextDouble() * 3.141f/4f;
                if (_rand.NextDouble() <= 0.5f)
                    angle *= -1;
            }
            Vector2 spawnDir = new Vector2(radius, 0f).RotateBy(angle);
            Console.WriteLine(radius);
            return new Circle(parent.P + spawnDir, radius, angle);
        }

        private List<(int x, int y)> GetOverlappingTiles()
        {
            var tiles = new List<(int x, int y)>();

            foreach (var circle in _circles)
            {
                int r = (int)Math.Round(Math.Abs(circle.R));
                int xMin = (int)(circle.P.X < 0 ? Math.Floor(circle.P.X) : Math.Ceiling(circle.P.X)) - r;
                int xMax = (int)(circle.P.X < 0 ? Math.Floor(circle.P.X) : Math.Ceiling(circle.P.X)) + r;
                int yMin = (int)(circle.P.Y < 0 ? Math.Floor(circle.P.Y) : Math.Ceiling(circle.P.Y)) - r;
                int yMax = (int)(circle.P.Y < 0 ? Math.Floor(circle.P.Y) : Math.Ceiling(circle.P.Y)) + r;

                for (int x = xMin; x <= xMax; x++)
                    for (int y = yMin; y <= yMax; y++)
                        if ((new Vector2(x, y) - circle.P).Length() < circle.R)
                        {
                            if (!tiles.Contains((x, y)))
                                tiles.Add((x, y));
                        }
            }

            return tiles;
        }
    }
}