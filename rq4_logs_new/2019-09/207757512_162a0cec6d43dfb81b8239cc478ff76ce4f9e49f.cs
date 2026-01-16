using EventCatalogAPI.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EventCatalog.Data
{
    public class EventCatalogContext : DbContext
    {

        public EventCatalogContext(DbContextOptions options) : base(options)
        {

        }
        public DbSet<EventType> EventTypes { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EventType>(ConfigureEventType);
            modelBuilder.Entity<EventCategory>(ConfigureEventCategory);
            modelBuilder.Entity<EventLocation>(ConfigureEventLocation);
            modelBuilder.Entity<EventsCatalog>(ConfigureEventsCatalog);
        }

        private void ConfigureEventsCatalog(EntityTypeBuilder<EventsCatalog> builder)
        {
            builder.ToTable("Catalog");
            builder.Property(c => c.Id).IsRequired().ForSqlServerUseSequenceHiLo("events_hilo");
            builder.Property(c => c.Name).IsRequired().HasMaxLength(100);
            builder.Property(c => c.Address).IsRequired().HasMaxLength(250);
            builder.Property(c => c.Description).IsRequired().HasMaxLength(300);
            builder.Property(c => c.OrganizerName).IsRequired();
            builder.Property(c => c.OrganizerDescription).IsRequired().HasMaxLength(150);
            builder.Property(c => c.StartDateTime).IsRequired();
            builder.Property(c => c.EndDateTime).IsRequired();

            //ForeignKey relation with EventType
            builder.HasOne(e => e.EventType)
                .WithMany()
                .HasForeignKey(e => e.EventTypeId);

            //ForeignKey relation with EventCategory
            builder.HasOne(e => e.EventCategory)
                .WithMany()
                .HasForeignKey(e => e.EventCategoryId);

            //ForeignKey relation with EventLocation
            builder.HasOne(e => e.EventLocation)
                .WithMany()
                .HasForeignKey(e => e.EventLocationId);


        }

        private void ConfigureEventLocation(EntityTypeBuilder<EventLocation> builder)
        {
            builder.ToTable("EventLocations");
            builder.Property(l => l.Id).IsRequired()
                .ForSqlServerUseSequenceHiLo("event_location_hilo");

            builder.Property(l => l.City).IsRequired().HasMaxLength(100);
        }

        private void ConfigureEventCategory(EntityTypeBuilder<EventCategory> builder)
        {
            builder.ToTable("EventCategories");
            builder.Property(c => c.Id).IsRequired()
                .ForSqlServerUseSequenceHiLo("event_category_hilo");

            builder.Property(c => c.Name).IsRequired();
        }

        private void ConfigureEventType(EntityTypeBuilder<EventType> builder)
        {
            builder.ToTable("EventTypes");
            builder.Property(t => t.Id)
                .IsRequired()
                .ForSqlServerUseSequenceHiLo("event_types_hilo");

            builder.Property(t => t.Type)
                .IsRequired().HasMaxLength(100);
        }
    }
}