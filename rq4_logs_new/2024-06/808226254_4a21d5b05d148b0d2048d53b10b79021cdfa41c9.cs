using ActivityFinder.Server.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace ActivityFinder.Server.Database
{
    public sealed class SoftDeleteInterceptor : SaveChangesInterceptor
    {
        public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
        {
            if (eventData.Context != null)
                SoftDeleteEntities(eventData);

            return base.SavingChanges(eventData, result);          
        }

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (eventData.Context != null)
                SoftDeleteEntities(eventData);

            return base.SavingChangesAsync(eventData, result, cancellationToken);           
        }

        private void SoftDeleteEntities(DbContextEventData eventData)
        {
            IEnumerable<EntityEntry<IHasDbProperties>> entries = eventData.Context
                .ChangeTracker
                .Entries<IHasDbProperties>()
                .Where(e => e.State == EntityState.Deleted);

            foreach (EntityEntry<IHasDbProperties> softDeletableEntity in entries)
            {
                softDeletableEntity.State = EntityState.Modified;
                softDeletableEntity.Entity.DbProperties.Deleted = true;
                softDeletableEntity.Entity.DbProperties.DeleteTime = DateTime.Now;
            }
        }
    }
}