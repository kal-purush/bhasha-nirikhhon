using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Zandeross.Domain
{
    //abstract for referenced other classes
    public abstract class BaseDataEntities
    {
        
        [Key,DatabaseGenerated(DatabaseGeneratedOption.Identity),Column("Id")] 
        public int Id { get; set; }
    }
}