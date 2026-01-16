using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ScalabiltyHomework.Data.Entity
{
    public class Hero
    {
        public Hero()
        {
            PromotionDate = DateTime.Now;
        }

        public Hero(int id, string comment)
        {
            PersonId = id;
            Comment = comment;
        }

        public Hero(Person person, string comment)
        {
            Person = person;
            PersonId = person.Id;
            Comment = comment;
        }

        public int Id { get; set; }
        public int PersonId { get; set; }
        [Required]
        public string Comment { get; set; }
        public DateTime PromotionDate { get; set; }

        [ForeignKey("PersonId")]
        public virtual Person Person { get; set; }
    }
}