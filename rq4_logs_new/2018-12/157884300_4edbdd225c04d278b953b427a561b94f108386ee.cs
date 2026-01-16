using planit_data.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace planit_data.DTOs
{
    public class CreateBoardDTO
    {
        public String Name { get; set; }
        public int CreatedByUser { get; set; }

        public Board FromDTO()
        {
            return new Board()
            {
                Name = Name
            };
        }
    }

    public class UpdateBoardDTO
    {
        public String Name { get; set; }
        public int BoardId { get; set; }
    }

    public class ReadBoardDTO
    {
        public int BoardId { get; set; }
        public String Name { get; set; }

        public ReadBoardDTO(Board b)
        {
            this.BoardId = b.BoardId;
            this.Name = b.Name;
        }

        public static List<ReadBoardDTO> FromEntityList(List<Board> boards)
        {
            List<ReadBoardDTO> list = new List<ReadBoardDTO>();
            foreach (Board b in boards)
            {
                list.Add(new ReadBoardDTO(b));
            }
            return list;
        }
    }
}