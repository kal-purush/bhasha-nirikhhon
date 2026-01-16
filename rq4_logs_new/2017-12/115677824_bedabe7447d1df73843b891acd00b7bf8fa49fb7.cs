using System.Collections.Generic;
using System.Threading.Tasks;
namespace RayTracer {
    public class Camera {
        public Vector Position;
        public Vector Target;
        public Vector Up;
        public Vector U, V, W;
        public int MaxDepth;
        public ViewPlane ViewPlane;
        public Tracer Tracer;

        public Camera(
                Tracer tracer,
                ViewPlane viewPlane,
                Vector position,
                Vector target,
                Vector up,
                int maxDepth = 4) {
            Position = position;
            Target = target;
            Up = up;
            Tracer = tracer;
            ViewPlane = viewPlane;
            MaxDepth = maxDepth;

            UpdateUVW();
        }

        private void UpdateUVW() {
            W = (Position - Target).Normalized();
            U = (Up ^ W).Normalized();
            V = W ^ U;
        }

        private Ray InitRay(double x, double y) {
            Vector direction = (U * x + V * y - W * ViewPlane.Distance).Normalized();
            return new Ray(Position, direction);
        }

        public Color[,] Render(int samples_count=2) {
            var pixels = new Color[ViewPlane.Width, ViewPlane.Height];
            var samples = Samples(samples_count);

            Parallel.For(0, ViewPlane.Width,
                (x) => {
                    for (var y = 0; y < ViewPlane.Height; y++) {
                        var result = new Color();

                        foreach (var sample in samples) {
                            var X = ViewPlane.Ratio * (x - 0.5 * ViewPlane.Width + sample.X) / (double) ViewPlane.Width;
                            var Y = (y - 0.5 * ViewPlane.Height + sample.Y) / (double) ViewPlane.Height;
                            var ray = InitRay(X, Y);
                            result += Tracer.trace(ray, MaxDepth);
                        }
                        result /= (double) samples.Count;
                        pixels[x, y] = result;
                    }
                }
            );
            return pixels;
        }

        protected List<Vector> Samples(int count) {
            var result = new List<Vector>();
            for (var i = 0; i < count; i++) {
                for (var j = 0; j < count; j++) {
                    result.Add(new Vector((i + 0.5) / (double)count, (j + 0.5) / (double)count, 0.0));
                }
            }
            return result;
        }
    }
}