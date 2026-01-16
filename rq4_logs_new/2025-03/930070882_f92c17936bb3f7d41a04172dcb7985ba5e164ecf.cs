using System.ComponentModel.DataAnnotations;

namespace Padel.Application.DTOs.Productos;

public record CreateProductDto(
    [MaxLength(50)] string Name,
    decimal Price
);