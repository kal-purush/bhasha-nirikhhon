using LogCollections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LogViewer
{
    public class LogEntryView
    {
        public int Id { get; set; }
        public byte Meta { get; set; }
        public Guid Key { get; set; }
        public string Value { get; set; }

        public static implicit operator LogEntryView(in LogEntry entry)
        {
            return new LogEntryView
            {
                Id = entry.Id,
                Meta = entry.Meta,
                Key = entry.Key,
                Value = System.Text.Encoding.UTF8.GetString(entry.Value)
            };
        }
    }
}