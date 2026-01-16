using System;
using System.IO;

namespace LegalLead.PublicData.Search.Models
{
    public class SearchContext
    {
        public string Id { get; set; }
        public string Content 
        { 
            get 
            { 
                if (!string.IsNullOrEmpty(_content)) return _content;
                if (!File.Exists(LocalFileName)) return _content;
                // Read all bytes from the file
                var fileBytes = File.ReadAllBytes(LocalFileName);
                _content = Convert.ToBase64String(fileBytes);
                return _content;
            } 
        }
        public string LocalFileName { get; set; }
        public string FileFormat { get; set; } = "EXL";
        public string FileStatus { get; set; } = "NONE";
        private string _content = string.Empty;
    }
}