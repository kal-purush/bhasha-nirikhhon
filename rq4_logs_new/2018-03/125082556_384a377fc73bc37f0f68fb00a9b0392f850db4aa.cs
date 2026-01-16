using Newtonsoft.Json;
namespace WriteReadSameFileThreading.DataModel
{
    /// <summary>
    /// Sample class structure used by <see cref="Program"/> class to create read and write threads to process this kind of data
    /// </summary>
    public class SampleData
    {
        /// Summary:
        ///     Initialization
        public SampleData(int id, string details = null, string time = null)
        {
            Id = id;
            Details = details;
            Time = time;
        }

        public int Id { get; set; }
        
        public string Details { get; set; }

        public string Time { get; set; }

        [JsonIgnore]  //To ignore from serialization use this attribute
        public string NotMapped { get; set; }

        //
        // Summary:
        //     Returns a string that represents the current object.
        //
        // Returns:
        //     A string that represents the current object.
        public override string ToString()
        {
            return $"{Id} ---  {Details} ---  {Time}";
        }
    }
}