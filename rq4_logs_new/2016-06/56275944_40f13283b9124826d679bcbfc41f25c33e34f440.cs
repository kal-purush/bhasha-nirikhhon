using SuperERP.DAL.Models;
using System.Data.Entity.ModelConfiguration;

namespace SuperERP.DAL.Mapping
{
    public class ContatoMap : EntityTypeConfiguration<Contato>
    {
        public ContatoMap()
        {
            // Primary Key
            this.HasKey(t => t.Id);

            // Properties
            this.Property(t => t.Nome)
                .IsRequired()
                .HasMaxLength(60);

            this.Property(t => t.Email)
                .IsRequired()
                .HasMaxLength(64);

            this.Property(t => t.Fone)
                .IsRequired()
                .HasMaxLength(15);

            this.Property(t => t.Cargo)
                .HasMaxLength(30);

            // Table & Column Mappings
            this.ToTable("Contato");
            this.Property(t => t.Id).HasColumnName("ID");
            this.Property(t => t.Nome).HasColumnName("Nome");
            this.Property(t => t.IdPessoaJuridica).HasColumnName("ID_PessoaJuridica");
            this.Property(t => t.IdPessoaFisica).HasColumnName("ID_PessoaFisica");
            this.Property(t => t.Email).HasColumnName("Email");
            this.Property(t => t.Fone).HasColumnName("Fone");
            this.Property(t => t.Cargo).HasColumnName("Cargo");

            // Relationships
            this.HasOptional(t => t.PessoaFisica)
                .WithMany(t => t.Contatos)
                .HasForeignKey(d => d.IdPessoaFisica);
            this.HasOptional(t => t.PessoaJuridica)
                .WithMany(t => t.Contatoes)
                .HasForeignKey(d => d.IdPessoaJuridica);
        }
    }
}