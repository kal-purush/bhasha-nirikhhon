using System.Collections.Generic;
using System.Runtime.Serialization;

namespace HighFreqUpdate
{
    [DataContract]
    public class GridCustomizations
    {
        [DataMember]
        public string GridLayout { get; set; } //here we  will put grid's layout writen in base64

        [DataMember]
        public IDictionary<string, ColumnSettings> ColumnsStyle { get; set; }

        public GridCustomizations()
        {
            ColumnsStyle = new Dictionary<string, ColumnSettings>();
        }
    }

    [DataContract]
    public class ColumnSettings
    {
        [DataMember]
        public string ForeColor { get; set; }
        [DataMember]
        public string BackGroundColor { get; set; }

        public bool HasData
        {
            get
            {
                return !string.IsNullOrEmpty(BackGroundColor) ||
                       !string.IsNullOrEmpty(ForeColor);
            }
        }
    }
}