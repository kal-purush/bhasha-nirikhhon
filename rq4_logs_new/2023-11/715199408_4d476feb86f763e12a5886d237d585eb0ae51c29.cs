using NerdStore.Catalog.Domain.Models;
using NerdStore.Catalog.Domain.ValueObjects;
using NerdStore.Core.DomainObjects.Exceptions;

namespace NerdStore.Catalog.Domain.Tests;

public class ProductTests
{
    [Fact]
    public void Product_Validate_ValidationsShouldReturnExceptions()
    {
        var ex = Assert.Throws<DomainException>(() =>
        {
            new Product(Guid.NewGuid(), string.Empty, "Descricao", false, 100, DateTime.Now, "Imagem", new Dimension(1, 1, 1));
        });

        Assert.Equal("O campo Nome do produto não pode estar vazio", ex.Message);

        ex = Assert.Throws<DomainException>(() =>
        {
            new Product(Guid.NewGuid(), "Nome", string.Empty, false, 100, DateTime.Now, "Imagem", new Dimension(1, 1, 1));
        });

        Assert.Equal("O campo Descricao do produto não pode estar vazio", ex.Message);

        ex = Assert.Throws<DomainException>(() =>
        {
            new Product(Guid.NewGuid(), "Nome", "Descricao", false, 0, DateTime.Now, "Imagem", new Dimension(1, 1, 1));
        });

        Assert.Equal("O campo Valor do produto não pode se menor igual a 0", ex.Message);

        ex = Assert.Throws<DomainException>(() =>
        {
            new Product(Guid.Empty, "Nome", "Descricao", false, 100, DateTime.Now, "Imagem", new Dimension(1, 1, 1));
        });

        Assert.Equal("O campo CategoriaId do produto não pode estar vazio", ex.Message);

        ex = Assert.Throws<DomainException>(() =>
        {
            new Product(Guid.NewGuid(), "Nome", "Descricao", false, 100, DateTime.Now, string.Empty, new Dimension(1, 1, 1));
        });

        Assert.Equal("O campo Imagem do produto não pode estar vazio", ex.Message);

        ex = Assert.Throws<DomainException>(() =>
        {
            new Product(Guid.NewGuid(), "Nome", "Descricao", false, 100, DateTime.Now, "Imagem", new Dimension(0, 1, 1));
        });

        Assert.Equal("O campo Altura não pode ser menor ou igual a 0", ex.Message);
    }
}