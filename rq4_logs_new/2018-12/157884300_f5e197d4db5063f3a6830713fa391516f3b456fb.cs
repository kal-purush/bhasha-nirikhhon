using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using planit_data.Entities;

namespace planit_data.DTOs
{
    public class CreateCardDTO
    {
        public String Name { get; set; }
        public String Description { get; set; }
        public DateTime TimeStamp { get; set; }
        public DateTime DueDate { get; set; }
        public int ListId { get; set; }
        public int UserId { get; set; }
    }

    public class ReadCardDTO
    {
        public int CardId { get; set; }
        public String Name { get; set; }
        public String Description { get; set; }
        public DateTime TimeStamp { get; set; }
        public DateTime DueDate { get; set; }
        public CardList List { get; set; }
    }
}