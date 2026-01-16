namespace ISQExplorer.Models
{
    public interface IRangedModel
    {
        Season? Season { get; set; }
        int? Year { get; set; }
    }
}