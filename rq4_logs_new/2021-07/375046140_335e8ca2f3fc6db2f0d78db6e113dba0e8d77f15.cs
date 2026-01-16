using System;
using Core.Entities.Halko;

namespace Core.Specifications
{
    public class TransactionSpecification : BaseSpecification<Transaction>
    {
        public TransactionSpecification( DateTime date, string pointName ) 
            : base( x => x.DateTime.Date == date 
                         && x.Point.Name == pointName )
        {
            AddInclude ( x => x.Participant );
            AddInclude ( x => x.ProductCategory );
            AddInclude ( x => x.TransactionType );
            AddInclude ( x => x.Point );
        }
    }
}