using Microsoft.EntityFrameworkCore;
using TsundokuTraducoes.Data.Context;
using TsundokuTraducoes.Data.Repositories;
using TsundokuTraducoes.Domain.Interfaces.Repositories;
using TsundokuTraducoes.Entities.Entities.Obra;
using TsundokuTraducoes.Helpers;
using TsundokuTraducoes.Domain.Services;

namespace TsundokuTraducoes.Infrastructure.Data.Repositories;

public class ObrasRepositoryTestes
{
    private Novel GerarNovel()
    {
        var novel = new Novel();
        novel.AdicionaNovel(
            Guid.NewGuid(),
            "Bruxa Errante, a Jornada dos Testes",
            "Majo no Tabitabi, The Journey of Elaina, The Witch's Travels, 魔女の旅々",
            "Bruxa Errante",
            "Shiraishi Jougi",
            "Azure",
            "2017",
            "bruxa-errante-a-jornada-dos-testes",
            "Bravo",
            "Bravo",
            "https://tsundoku.com.br/wp-content/uploads/2021/12/MJ_V8_Capa.jpg",
            "A Bruxa, Sim, sou eu.",
            DateTime.Now,
            DateTime.Now,
            false,
            false,
            "#81F7F3",
            "https://tsundoku.com.br/wp-content/uploads/2021/12/testeBanner.jpg",
            "@Bruxa Errante, a Jornada de Elaina",
            Diretorios.RetornaDiretorioImagemCriado("BruxaErrante"),
            "em-andamento",
            "light-novel",
            "japonesa",
            "Uma obra muito boa");
            
        return novel;
    }

    [Fact]
    public async Task ObterNovelPorId_DeveRetornarNovelQuandoIdValido()
    {
        // Arrange
        var id = Guid.NewGuid();
        var novel = GerarNovel();
        novel.Id = id;

        var options = new DbContextOptionsBuilder<ContextBase>()
            .UseInMemoryDatabase(databaseName: "TestDatabase")
            .Options;

        await using (var context = new ContextBase(options))
        {
            IObraRepository repositoryAdmin = new ObraRepository
                (context, 
                new GeneroDeParaRepository(context),
                new GeneroRepository(context));

            var serviceAdmin = new ObraService(repositoryAdmin);
            await serviceAdmin.AdicionaNovel(novel);
            
            // Act
            var repositoryPublic = new ObrasRepository(context);
            var resultado = await repositoryPublic.ObterNovelPorId(id);

            // Assert
            Assert.NotNull(resultado);
            Assert.Equal(id, resultado.Id);
        }
        
        await using (var context = new ContextBase(options))
        {
            await context.Database.EnsureDeletedAsync();
        }
    }

    [Fact]
    public async Task ObterNovelPorId_DeveRetornarNullQuandoNovelNaoExiste()
    {
        // Arrange
        var idGeradoExternamente = Guid.NewGuid();
        var novel = GerarNovel();

        var options = new DbContextOptionsBuilder<ContextBase>()
            .UseInMemoryDatabase(databaseName: "TestDatabase")
            .Options;


        await using (var context = new ContextBase(options))
        {
            IObraRepository repositoryAdmin = new ObraRepository
            (context, 
                new GeneroDeParaRepository(context),
                new GeneroRepository(context));
            
            var serviceAdmin = new ObraService(repositoryAdmin);
            await serviceAdmin.AdicionaNovel(novel);
            
            // Act
            var repositoryPublic = new ObrasRepository(context);
            var resultado = await repositoryPublic.ObterNovelPorId(idGeradoExternamente);

            // Assert
            Assert.Null(resultado);
        }
        
        // Clean up
        await using (var context = new ContextBase(options))
        {
            await context.Database.EnsureDeletedAsync();
        }
    }

    [Fact]
    public async Task ObterNovelPorSlug_DeveRetornarNovelQuandoSlugValido()
    {
        // Arrange
        var slug = "slug-teste";
        var novel = GerarNovel();
        novel.Slug = slug;

        var options = new DbContextOptionsBuilder<ContextBase>()
            .UseInMemoryDatabase(databaseName: "TestDatabase")
            .Options;


        await using (var context = new ContextBase(options))
        {
            IObraRepository repositoryAdmin = new ObraRepository
                (context, 
                new GeneroDeParaRepository(context),
                new GeneroRepository(context));
            
            var serviceAdmin = new ObraService(repositoryAdmin);
            await serviceAdmin.AdicionaNovel(novel);
            
            // Act
            var repositoryPublic = new ObrasRepository(context);
            var resultado = await repositoryPublic.ObterNovelPorSlug(slug);

            // Assert
            Assert.NotNull(resultado);
            Assert.Equal(slug, resultado.Slug);
        }
        
        // Clean up
        await using (var context = new ContextBase(options))
        {
            await context.Database.EnsureDeletedAsync();
        }
    }

    [Fact]
    public async Task ObterNovelPorSlug_DeveRetornarNullQuandoNovelNaoExiste()
    {
        // Arrange
        var slug = "slug-teste";
        // slug da novel = 'bruxa-errante-a-jornada-dos-testes'
        var novel = GerarNovel();

        var options = new DbContextOptionsBuilder<ContextBase>()
            .UseInMemoryDatabase(databaseName: "TestDatabase")
            .Options;


        await using (var context = new ContextBase(options))
        {
            IObraRepository repositoryAdmin = new ObraRepository
            (context, 
                new GeneroDeParaRepository(context),
                new GeneroRepository(context));
            
            var serviceAdmin = new ObraService(repositoryAdmin);
            await serviceAdmin.AdicionaNovel(novel);
            
            // Act
            var repositoryPublic = new ObrasRepository(context);
            var resultado = await repositoryPublic.ObterNovelPorSlug(slug);

            // Assert
            Assert.Null(resultado);
        }
        
        // Clean up
        await using (var context = new ContextBase(options))
        {
            await context.Database.EnsureDeletedAsync();
        }
    }
 
    [Fact]
    public void TrataRetornoNovelUnica_DeveRetornarObjetoEsperado()
    {
        // Arrange
        var novel = GerarNovel();
        
        // Act
        var resultado = ObrasRepository.TrataRetornoNovelUnica(novel);

        // Assert
        Assert.NotNull(resultado);
        Assert.Equal("Bruxa Errante, a Jornada dos Testes", resultado.Titulo);
        Assert.Equal("Light Novel", resultado.TipoObra);
        Assert.Equal("Shiraishi Jougi", resultado.Autor);
    }
}