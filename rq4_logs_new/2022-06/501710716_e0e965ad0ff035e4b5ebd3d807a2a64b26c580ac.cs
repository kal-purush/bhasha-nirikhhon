using System.Collections.ObjectModel;
using CollectionManagementAPI.Entity;
using Microsoft.EntityFrameworkCore;

namespace CollectionManagementAPI.DataAccess;

public class CollectionManagementDbContext : DbContext
{
    
    public CollectionManagementDbContext(DbContextOptions<CollectionManagementDbContext> options) :
        base(options)
    {
        
    }
    
    public DbSet<CollectionEntity> Collections { get; set; }
    
    public DbSet<ElementEntity> Elements { get; set; }
    
    public DbSet<TagsEntity> Tagss { get; set; }
    
    public DbSet<UserEntity> Users { get; set; }
}