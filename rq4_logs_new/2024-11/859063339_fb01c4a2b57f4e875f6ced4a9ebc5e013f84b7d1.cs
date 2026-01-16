using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TripEnjoy.Application.Data
{
    // using for response Comment with account name , image , account id
    // 
    public class CommentResponse
    {
        public int CommentId { get; set; }
        public int AccountId { get; set; }
        public int RoomId { get; set; }
        public string CommentContent { get; set; }
        public int CommentLevel { get; set; }
        public string CommentReply { get; set; }
        public DateTime CommentDate { get; set; }
        public string AccountName { get; set; }
        public string AccountImage { get; set; }
    }
}