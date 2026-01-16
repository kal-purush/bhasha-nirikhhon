using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;
using Countersoft.Gemini.Commons.Dto;
using Countersoft.Gemini.Models;

namespace DocStore.App
{
    public class DocumentsModel : BaseModel
    {
        public List<ProjectDocumentDto> FolderList { get; set; }
        public bool ReadOnly { get; set; }
        public IEnumerable<SelectListItem> ProjectList { get; set; }
        public int SelectedFolder { get; set; }
    }
}