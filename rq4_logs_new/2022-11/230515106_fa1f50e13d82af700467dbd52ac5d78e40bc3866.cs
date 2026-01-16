using System;

namespace BeerStuff.DataAccess.Entities.BeerNetContext.Models
{
    public partial class BeerAdjunct
    {
        private DateTime _rowCreated;
        private DateTime _rowModified;

        public DateTime RowCreated
        {
            get =>
                (_rowCreated.Kind == DateTimeKind.Unspecified)
                    ? DateTime.SpecifyKind(_rowCreated, DateTimeKind.Utc)
                    : _rowCreated;

            set => _rowCreated = value;
        }

        public DateTime RowModified
        {
            get =>
                (_rowModified.Kind == DateTimeKind.Unspecified)
                    ? DateTime.SpecifyKind(_rowModified, DateTimeKind.Utc)
                    : _rowModified;

            set => _rowModified = value;
        }
    }
}