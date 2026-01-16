using System;
using Core.Entities.Halko;

namespace Core.Specifications
{
    public class TransactionDeletedSpecification : BaseSpecification<TransactionDeleted>
    {
        public TransactionDeletedSpecification( DateTime date, string pointName )
            : base ( x => x.InsertedDateTime.Date == date && x.Point.Name == pointName )
        {
            AddInclude ( x => x.Participant );
            AddInclude ( x => x.ProductCategory );
            AddInclude ( x => x.TransactionType );
            AddInclude ( x => x.Point );
        }

        public TransactionDeletedSpecification( string pointName )
            : base ( x => x.Point.Name == pointName )
        {
            AddInclude ( x => x.Participant );
            AddInclude ( x => x.ProductCategory );
            AddInclude ( x => x.TransactionType );
            AddInclude ( x => x.Point );
        }
    }
}