using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Projet_Mines_Official
{
    public class Permis_ElementDossier
    {
        public Permis_ElementDossier()
        {
            this.isExist = false;
        }
        [Key]
        [Column(Order =1)]
        public int PermisId { get; set; }
        [ForeignKey("PermisId")]
        public virtual Permis Permis { get; set; }

        [Key]
        [Column(Order = 2)]
        public int Element_DossierId { get; set; }
        [ForeignKey("Element_DossierId")]
        public virtual Element_Dossier Element_Dossier { get; set; }

        public bool isExist { get; set; }
    }
}