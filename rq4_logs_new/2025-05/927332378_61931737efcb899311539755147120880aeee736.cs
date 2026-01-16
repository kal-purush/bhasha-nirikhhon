namespace mangadex.Models;

public class MangaAttributes
{
    public Dictionary<string, string> Title { get; set; }
    
    public Dictionary<string, string> Description { get; set; }
    public required string Status { get; set; }
    
    public string? OriginalLanguage { get; set; }
    
    public required string ContentRating { get; set; }

    public string ObterTitulo()
    {

        if (Title.TryGetValue("pt-br", out var tituloPt))
            return tituloPt;

        if (Title.TryGetValue("en", out var tituloEn))
            return tituloEn;

        return Title.Values.FirstOrDefault() ?? "Título não disponível";
    }
    
    public string ObterDescricao()
    {

        if (Description.TryGetValue("pt-br", out var descricaoPt))
            return descricaoPt;

        if (Description.TryGetValue("en", out var descricaoEn))
            return descricaoEn;

        return Description.Values.FirstOrDefault() ?? "Descrição não disponível";
    }
}