using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace News.Entity
{
    public class NewsEntity
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Txt { get; set; }

        public NewsEntity(int _id, string _title, string _txt)
        {
            this.Id = _id;
            this.Title = _title;
            this.Txt = _txt;
        }
    }
}