

namespace Triangulos.ConsoleApp.Controladores
{
    public class ControladorTriangulo
    {
        private TrianguloBase triangulo;

        public void RegistrarTriangulo(double ladoX, double ladoY, double ladoZ)
        {
            TrianguloBase novoTriangulo = new TrianguloBase();

            novoTriangulo.ladoX = ladoX;
            novoTriangulo.ladoY = ladoY;
            novoTriangulo.ladoZ = ladoZ;

            triangulo = novoTriangulo;
        }

        public TrianguloBase DefinirTriangulo()
        {
            if (triangulo.Validar() == false)
                return null;

            if (triangulo.ladoX == triangulo.ladoY && triangulo.ladoX == triangulo.ladoZ &&
                triangulo.ladoY == triangulo.ladoZ)
                triangulo = new TrianguloEquilatero();

            else if (triangulo.ladoX == triangulo.ladoY || triangulo.ladoX == triangulo.ladoZ ||
                triangulo.ladoY == triangulo.ladoZ)
                triangulo = new TrianguloIsosceles();

            else if (triangulo.ladoX != triangulo.ladoY && triangulo.ladoX != triangulo.ladoZ &&
                triangulo.ladoY != triangulo.ladoZ)
                triangulo = new TrianguloEscaleno();

            return triangulo;
        }
    }
}