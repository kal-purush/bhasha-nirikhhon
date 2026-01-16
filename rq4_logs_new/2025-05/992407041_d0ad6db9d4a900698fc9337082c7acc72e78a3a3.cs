public class Carrito
{
    public int productoid { get; set; }
    public int cantidad { get; set; }
    public required string titulo { get; set; }
    public decimal precio { get; set; }
    public required List<string> categorias { get; set; }
    public int? archivoId { get; set; }
}