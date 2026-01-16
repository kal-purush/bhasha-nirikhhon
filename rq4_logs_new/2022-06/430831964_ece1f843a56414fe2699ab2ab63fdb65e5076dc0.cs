using System.Net;
using AutoMapper;
using Infrastructure.Models.Products;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Infrastructure.MediatR.Products.Commands;

public class DeleteProductByArticulRequest : IRequest<ProductDto>
{
    public string Articul { get; set; } = null!;
}

public class DeleteProductByArticulHandler : IRequestHandler<DeleteProductByArticulRequest, ProductDto>
{
    private readonly MContext _context;
    private readonly IMapper _mapper;
    private readonly ILogger<DeleteProductByIdHandler> _logger;

    public DeleteProductByArticulHandler(MContext context, IMapper mapper, ILogger<DeleteProductByIdHandler> logger)
    {
        _context = context;
        _mapper = mapper;
        _logger = logger;
    }

    public async Task<ProductDto> Handle(DeleteProductByArticulRequest request, CancellationToken cancellationToken)
    {
        Product? product = await _context.Products
            .SingleOrDefaultAsync(c => c.Articul == request.Articul, cancellationToken);

        if (product is null)
        {
            throw new HttpRequestException($"Product with {request.Articul} articul not found", null, HttpStatusCode.NotFound);
        }

        _context.Remove(product);
        await _context.SaveChangesAsync(cancellationToken);
        
        _logger.LogInformation("Deleted product with {Id} id and {Articul} articul", product.Id, product.Articul);

        return _mapper.Map<ProductDto>(product);
    }
}