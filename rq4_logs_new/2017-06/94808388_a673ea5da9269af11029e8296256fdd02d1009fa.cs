using SFML.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Presentation
{
    class Graph : Drawable
    {
        public List<CircleShape> Vertices = new List<CircleShape>();
        public List<Tuple<int, int>> Edges = new List<Tuple<int, int>>();
        public VertexArray e, win, best;
        Random r;

        public List<List<double>> matrix;

        const uint SIZE = 16;
        int far = 50;
        int inf = -1;

        public Graph(int n, int m, int random)
        {
            e = new VertexArray() { PrimitiveType = PrimitiveType.Lines };
            win = new VertexArray() { PrimitiveType = PrimitiveType.Lines };
            best = new VertexArray() { PrimitiveType = PrimitiveType.Lines };

            m = m > (n - 1) * n / 2 ? (n - 1) * n / 2 : m; // maximum of edges

            if (random != 0)
                r = new Random(random);
            else
                r = new Random();

            //Initialize
            MakeDoubleMatrix(n);
            MakeVertices(n);
            MakeEdges(m, n);
            RecalculateDistance();
            ZeroToInfinity();
        }

        public Graph(int n, int m) : this(n, m, 0) { }


        public void MakeDoubleMatrix(int n)
        {
            matrix = new List<List<double>>();
            List<double> tmp = new List<double>();
            for (int i = 0; i < n; i++)
                tmp.Add(0);

            for (int i = 0; i < n; i++)
                matrix.Add(new List<double>(tmp));

        }

        //TODO niech ma argument arrayvertex
        public void ColorPath(List<Tuple<int, int>> list)
        {
            win.Clear();
            for (int i = 0; i < list.Count; i++)
            {
                int u = list[i].Item1;
                int v = list[i].Item2;
                Vertex x = new Vertex(Vertices[u].Position);
                x.Color = Color.Green;
                win.Append(x);
                Vertex t = new Vertex(Vertices[v].Position);
                t.Color = Color.Green;
                win.Append(t);
            }
        }

        //Tego się pozbyć
        public void ColorPathBest(List<Tuple<int, int>> list)
        {
            best.Clear();
            for (int i = 0; i < list.Count; i++)
            {
                int u = list[i].Item1;
                int v = list[i].Item2;
                Vertex x = new Vertex(Vertices[u].Position);
                x.Color = Color.Yellow;
                best.Append(x);
                Vertex t = new Vertex(Vertices[v].Position);
                t.Color = Color.Yellow;
                best.Append(t);
            }
        }
        
        void ZeroToInfinity()
        {
            for (int i = 0; i < matrix.Count; i++)
                for (int j = 0; j < matrix[i].Count; j++)
                    if(matrix[i][j] == 0)
                        matrix[i][j] = inf;
        }

        public void RecalculateDistance()
        {
            Edges.Sort();
            int id = 0;
            Tuple<int, int> t = Edges[id];
            for (int i = 0; i < matrix.Count; i++)
            {
                for (int j = 0; j < matrix[i].Count; j++)
                {
                    if (matrix[i][j] != inf && (t.Item1 == i && t.Item2 == j || t.Item1 == j && t.Item2 == i))
                    {
                        double dist = Distance(Vertices[i].Position, Vertices[j].Position);
                        matrix[i][j] = dist;
                        matrix[j][i] = dist;
                        id++;
                        t = id < Edges.Count ? Edges[id] : new Tuple<int, int>(-1, -1);
                    }
                }
            }
        }

        // tworzenie wierzchołków
        void MakeVertices(int n)
        {
            for (int i = 0; i < n; i++)
            {
                Vertices.Add(new CircleShape(SIZE, SIZE));
                int x = r.Next(100, 700), y = r.Next(100, 500);
                while (Vertices.Exists(v => Distance(new SFML.System.Vector2f(x, y), v.Position) < far))
                {
                    x = r.Next(100, 700);
                    y = r.Next(100, 500);
                }
                Vertices[i].Position = new SFML.System.Vector2f(x, y);
                Vertices[i].FillColor = Color.Blue;
                Vertices[i].Origin = new SFML.System.Vector2f(SIZE, SIZE);
                Vertices[i].OutlineColor = Color.Magenta;
                Vertices[i].OutlineThickness = 2;
            }
        }

        // generowanie i tworzenie połączeń
        void MakeEdges(int m, int n)
        {
            for (int i = 0; i < m; i++)
            {
                int u = r.Next(0, n);
                int v = r.Next(0, n);
                while (u == v || Edges.Exists(ed => (ed.Item1 == u && ed.Item2 == v) || (ed.Item1 == v && ed.Item2 == u)))
                {
                    u = r.Next(0, n);
                    v = r.Next(0, n);
                }
                Edges.Add(new Tuple<int, int>(u, v));
                Vertex x = new Vertex(Vertices[u].Position);
                x.Color = Color.Blue;
                e.Append(x);
                Vertex t = new Vertex(Vertices[v].Position);
                t.Color = Color.Blue;
                e.Append(t);
            }
        }

        public void ColorEdges(SFML.System.Vector2f v2f, Color c)
        {
            for (uint i = 0; i < e.VertexCount; i++)
                if (e[i].Position == v2f)
                    e[i] = new Vertex(v2f, c);
        }

        public void MoveEdges(SFML.System.Vector2f v2f, SFML.System.Vector2f v2ff)
        {
            for (uint i = 0; i < e.VertexCount; i++)
                if (e[i].Position == v2f)
                    e[i] = new Vertex(v2ff);
        }

        double Distance(SFML.System.Vector2f v1, SFML.System.Vector2f v2)
        {
            return Math.Pow(Math.Pow(v1.X - v2.X, 2) + Math.Pow(v1.Y - v2.Y, 2), 0.5);
        }

        public override string ToString()
        {
            string str = "";
            for (int i = 0; i < matrix.Count; i++)
            {
                str += (i < 10 ? "0" + i : i + "") + ": {";
                for (int j = 0; j < matrix[i].Count; j++)
                {
                    double x = matrix[i][j];
                    str += x.ToString("000") + (j == matrix[i].Count - 1 ? "" : ", ");
                }
                str += "}\r\n";
            }
            return str;
        }

        public void Draw(RenderTarget target, RenderStates states)
        {
            target.Draw(e);
            target.Draw(win);
            target.Draw(best);
            foreach (var item in Vertices)
            {
                target.Draw(item);
            }
        }
    }
}