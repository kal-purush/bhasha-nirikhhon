using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Linq;
using System.Data.SQLite;
using System.Text;
using SQLite;

namespace Stream_Browser.DB
{    
    class Stream
    {
        [PrimaryKey, AutoIncrement]
        public int id { get; set; }
        [MaxLength(512), Indexed]
        public string Game { get; set; }
        [MaxLength(512), Indexed]
        public string Broadcaster { get; set; }
        [MaxLength(512)]
        public string Url { get; set; }
        [MaxLength(1024)]
        public string Title { get; set; }
        public int Viewers { get; set; }
        public bool isSubscribed { get; set; }
        // Using https://github.com/praeclarum/sqlite-net/wiki/
    }
}