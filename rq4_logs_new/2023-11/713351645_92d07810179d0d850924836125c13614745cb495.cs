using System.ComponentModel.DataAnnotations;

namespace server.DTOs;

public class CreateEventDto
{
    [Required]
    public int Id { get; set; }
    [Required]
    public string Speakers { get; set; }
    [Required]
    public string Description { get; set; }
    [Required]
    [Timestamp]
    public string Time { get; set; }
    [Required]
    public string AuthorUsername { get; set; }
    [Required]
    public string Place { get; set; }
    [Required]
    public string PictureUrl { get; set; }
}