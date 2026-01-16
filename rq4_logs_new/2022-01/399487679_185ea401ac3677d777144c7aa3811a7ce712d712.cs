using System.Text.Json.Serialization;

namespace Igtampe.Neco.Common {

    /// <summary>Image stored on the database</summary>
    public class Image : AutomaticallyGeneratableIdentifiable {

        /// <summary>Person who uploaded this image</summary>
        public User? Uploader { get; set; }

        /// <summary>Data of this image</summary>
        [JsonIgnore]
        public byte[]? Data { get; set; }

        /// <summary>MIME Type of this image (image/png)</summary>
        public string Type { get; set; } = "";
    }
}