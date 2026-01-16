using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hackathon.AI.Models.Api
{
    public class GetIngestionIndexResponseModel
    {
        public string Name {  get; set; }
        public MetadataSchemaModel MetadataSchema { get; set; }
        public object UserData { get; set; }
        public IEnumerable<FeatureModel> Features { get; set; }
        public string ETag { get; set; }
        public DateTime CreatedDateTime { get; set; }
        public DateTime LastModifiedDateTime { get; set; }

    }
}