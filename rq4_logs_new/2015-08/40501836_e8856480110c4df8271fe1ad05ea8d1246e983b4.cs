using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity.ModelConfiguration;
using EfTransaction.Model;

namespace EfTransaction.Db.Configurations
{
    internal class AddressConfig : EntityTypeConfiguration<Address>
    {
        public AddressConfig()
        {
            HasKey(x => x.Id);
            Property(x => x.Id)
                .HasDatabaseGeneratedOption(DatabaseGeneratedOption.Identity);

            Property(x => x.StudentId)
                .HasColumnName("Student_Id");

            HasRequired(x => x.Student)
                .WithMany(l => l.Addresses)
                .HasForeignKey(x => x.StudentId)
                .WillCascadeOnDelete(false);
        }
    }
}