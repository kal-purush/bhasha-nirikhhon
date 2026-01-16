using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UtilityLibrary;


namespace CSR.Service.Entity
{
    [Table("[Tbl_Email]", true, false, "", "usp_SaveEmail", "usp_ReadEmail", "usp_UpdateEmail", "usp_DeleteEmail")]
    public class EmailEntyti
    {
        [Column(name: "Area", isDeleteParam: true, isUpdateParam: true, isAllowNull: false, isReadParam: true, isInsertParam: true, isPrimaryKey: true)]
        public string Area { get; set; }

        [Column(name: "To", isUpdateParam: true, isAllowNull: false, isInsertParam: true)]
        public string To { get; set; }
        [Column(name: "Subject", isUpdateParam: true, isAllowNull: false, isInsertParam: true)]
        public string Subject { get; set; }
        public string Message { get; set; }

    }
}