using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FindAnimal.Domain.Entities
{
    public abstract class Entity
    {
        public Guid Id { get; protected set; }

        protected Entity() { }
        protected Entity(Guid id) 
        {
            Id = id;
        }

        public override bool Equals(object? obj) 
        {
           if(obj == null || obj is not Entity) return false;

           if(ReferenceEquals(this, obj)) return true;

            Entity other = (Entity)obj;
            return Id == other.Id;
        }

        public static bool operator ==(Entity obj1, Entity obj2)
        {
            if(obj1 is null || obj2 is null) return false;
            
            return obj1.Equals(obj2); 
        }

        public static bool operator !=(Entity obj1, Entity obj2)
        {
            return !(obj1 == obj2);
        }

        public override int GetHashCode() 
        {
            return Id.GetHashCode();
        }
    }
}