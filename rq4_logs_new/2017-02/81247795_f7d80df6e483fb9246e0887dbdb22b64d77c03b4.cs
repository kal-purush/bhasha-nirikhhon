using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace LibarySystem.Core.Objects {

    public class LendedBook {

        [Key]
        public int Id { get; set; }

        public DateTime DateOfLend { get; set; }
        public DateTime? DateOfReturn { get; set; }

        [ForeignKey("Student")]
        public string StudentPesel { get; set; }

        public virtual Student Student { get; set; }

        [ForeignKey("Book")]
        public string CatalogueNumber { get; set; }

        public virtual Book Book { get; set; }

    }

}