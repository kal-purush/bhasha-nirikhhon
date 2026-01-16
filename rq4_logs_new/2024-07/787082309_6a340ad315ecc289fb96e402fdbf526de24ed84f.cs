using BoraRachar.Domain.Entity.Categorias;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace BoraRachar.Infra.Data.Repository.Orm.EntityMapConfigurations;

public class CategoriaConfiguration : IEntityTypeConfiguration<Categoria>
{
    public void Configure(EntityTypeBuilder<Categoria> builder)
    {
        builder.ToTable("Categorias");
        builder.HasKey(x => x.Id);

        builder.Property(x => x.Id)
            .HasColumnName("IdCategoria")
            .IsRequired();

        builder.Property(x => x.Descricao)
            .HasColumnName("Descricao")
            .IsRequired();

        builder.Property(x => x.DataCadastro)
            .HasColumnName("DataCadastro")
            .IsRequired();

        builder.Property(x => x.Status)
           .HasColumnName("Status")
           .IsRequired();
    }
}