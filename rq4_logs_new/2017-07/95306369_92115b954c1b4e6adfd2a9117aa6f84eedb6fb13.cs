

namespace StackOverflow
{
    using System;
    using System.Collections.Generic;
    
    public partial class Question
    {
        public int ID { get; set; }
        public int UserID { get; set; }
        public string Topic { get; set; }
        public string Text { get; set; }
        public string Name { get; set; }
    
        public virtual User User { get; set; }
    }
}