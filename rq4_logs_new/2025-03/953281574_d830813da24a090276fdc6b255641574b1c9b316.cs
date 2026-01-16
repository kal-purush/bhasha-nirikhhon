namespace GestionBiblioteca.Entities
{
    internal class Libro
    {
        private static int contador = 1;
        public int Id { get; set; }
        public string Titulo { get; set; }
        public string Autor { get; set; }
        public string ISBN { get; set; }
        public int PublicacionYear { get; set; }
        public bool Disponibe { get; set; }

        public Libro(string titulo, string autor, string isbn, int publicacionYear)
        {
            Id = contador;
            Titulo = titulo;
            Autor = autor;
            ISBN = isbn;
            PublicacionYear = publicacionYear;
            Disponibe = true;
            contador++;
        }

        public override string ToString()
        {
            return $"------------------------------------------------" + "\n" +
                   $"Id: {Id}" + "\n" +
                   $"Titulo: {Titulo}" + "\n" +
                   $"Autor: {Autor}" + "\n" +
                   $"ISBN: {ISBN}" + "\n" +
                   $"Año de publicación: {PublicacionYear}" + "\n" +
                   $"Disponible: {Disponibe}" + "\n" +
                   $"------------------------------------------------";
        }
    }
}