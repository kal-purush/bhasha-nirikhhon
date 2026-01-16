using System;
using System.Collections.Generic;
using System.Text;

namespace iRon.Core.Interfaces
{
    public interface IEntity<T,E> where T:struct where E:Enum 
    {
        T Id { get; set; }
        string Tag { get; set; }
        DateTimeOffset? CreatedDate { get; set; }
        DateTimeOffset? UpdatedDate { get; set; }
        T UpdatedById { get; set; }
        T CreatedById { get; set; }
        bool isNew { get; }
        bool isDirty { get; }
        string LegacyId { get; set; }
        E ObjectStatus { get; set; }
        
    }
}