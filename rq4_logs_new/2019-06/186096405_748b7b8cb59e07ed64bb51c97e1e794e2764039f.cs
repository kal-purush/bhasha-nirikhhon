using FileArchive.Core;
using FileArchive.Domain.Enums;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace FileArchive.Domain.Entities
{
    public class FileDetail: Entity
    {
        public Guid FileId { get; set; }
        public Entities.File File { get; set; }
        public Guid UserId { get; set; }
        public User User { get; set; }
        public int FolderId { get; set; }
        public Folder Folder { get; set; }

        public string Title { get; set; }
        public string Keyword { get; set; }
        public string OrginalFileName { get; set; }
        public string ExtentionFile { get; set; }
        public int FileTypeId { get; set; }
        public FileType FileType { get; set; }
        public Int64 FileSize { get; set; }

        //[DefaultValue(typeof( DateTime), DateTime.Now.ToString())]
        [Column(TypeName = "date")]
        public DateTime CreateDate { get; set; }
        public bool IsPreviewFolder { get; set; }
        public int RatingCount { get; set; }
        public int Rating { get; set; }

        public string CustomProperty1 { get; set; }
        public string CustomProperty2 { get; set; }
        public string CustomProperty3 { get; set; }
        public string CustomProperty4 { get; set; }
        public string CustomProperty5 { get; set; }
        public string CustomProperty6 { get; set; }
        public string CustomProperty7 { get; set; }
        public string CustomProperty8 { get; set; }
        public string CustomProperty9 { get; set; }
        public string CustomProperty10 { get; set; }

        public FileCustomPropertyStatus FileCustomPropertyStatusId1 { get; set; }
        public FileCustomPropertyStatus FileCustomPropertyStatusId2 { get; set; }
        public FileCustomPropertyStatus FileCustomPropertyStatusId3 { get; set; }
        public FileCustomPropertyStatus FileCustomPropertyStatusId4 { get; set; }
        public FileCustomPropertyStatus FileCustomPropertyStatusId5 { get; set; }
    }
}