using System;

namespace Warehouse.Core
{
    public interface IEntity 
    {
         int Id { get; set; }
    }

    public interface ISoftDelete
    {
        DateTime? Deleted { get; set; }
        bool IsDeleted { get; set; }
        string DeletedBy { get; set; }
    }
    public interface IState
    {
        DateTime Created { get; set; }
        DateTime Modified { get; set; }
        string CreatedBy { get; set; }
        string ModifiedBy { get; set; }
    }
}