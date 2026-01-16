using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities.Models
{
    public class QueueFileMetaDataModel
    {
        public FileMetaData FileMeta {get;set;}
        public String SocketID { get; set; }
        public String MappingID { get; set; }

        public QueueFileMetaDataModel()
        {

        }

        public QueueFileMetaDataModel(FileMetaData fileMeta, string socketID)
        {
            this.FileMeta = fileMeta;
            this.SocketID = socketID;
        }

        public QueueFileMetaDataModel(string socketID)
        {
            this.SocketID = socketID;
        }

        public QueueFileMetaDataModel(FileMetaData fileMeta)
        {
            this.FileMeta = fileMeta;
        }
    }
}