using Algenic.Data.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Algenic.Data.Configuration
{
    public class CompilationResultConfiguration : IEntityTypeConfiguration<CompilationResult>
    {
        public void Configure(EntityTypeBuilder<CompilationResult> builder)
        {
            builder.HasKey(c => c.SolutionId);

            /*builder.HasOne(c => c.Solution)
                .WithOne(s => s.CompilationResult)
                .HasForeignKey<CompilationResult>(c => c.SolutionId);*/
        }
    }
}