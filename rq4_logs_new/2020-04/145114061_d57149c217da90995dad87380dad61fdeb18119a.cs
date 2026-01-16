using System.Linq;
using Blog.Dal.Models.Base;

namespace Blog.Dal.Repositories.Base {

    public abstract class Sortable<T> : ISortable<T> where T : BaseEntity
    {
        public virtual IQueryable<T> Sort(IQueryable<T> entites, int filter, bool order)
        {
            if(order)
            {
                switch (filter)
                {
                    case 0 : {
                         return entites.OrderByDescending(x=> x.Id);
                    }                    
                    case 1 : {   
                        return entites.OrderByDescending(x => x.CreationDate);
                    }
                    case 2: {
                        return entites.OrderByDescending(x => x.ModificationDate);
                    }
                    
                    default: {
                        return entites;
                    }
                }
               
            }else {
                switch (filter)
                {
                    case 0 : {
                         return entites.OrderBy(x=> x.Id);
                    }                    
                    case 1 : {   
                        return entites.OrderBy(x => x.CreationDate);
                    }
                    case 2 : {
                        return entites.OrderBy( x => x.ModificationDate);
                    }
                    
                    default: {
                        return entites;
                    }
                }
            }
        }
    }
}