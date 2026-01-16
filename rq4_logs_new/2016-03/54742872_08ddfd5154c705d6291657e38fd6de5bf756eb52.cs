using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Education.SAT.Math.Models
{
    public class BsonDocumentViewModel
    {
        private BsonDocument mBsonDocument;

        public BsonDocumentViewModel(BsonDocument doc)
        {
            mBsonDocument = doc;
        }


        public string this[int idx]
        {
            get
            {
                if (idx < mBsonDocument.Count())
                    return mBsonDocument.GetValue(idx).ToString();
                else
                    return null;
            }
        }
    }
}