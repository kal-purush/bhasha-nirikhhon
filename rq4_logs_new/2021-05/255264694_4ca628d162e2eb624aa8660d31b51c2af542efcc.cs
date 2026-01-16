using System;

namespace insightcampus_api.Model
{
    public class ClassStudentModel
    {
        public int order_id { get; set; }
        public int order_user_seq { get; set; }
        public string name { get; set; }
        public string class_nm { get; set; }
        public int order_item_seq { get; set; }
        public DateTime order_date { get; set; }
        public DateTime start_date { get; set; }
        public DateTime end_date { get; set; }
        public string order_type { get; set; }
        public decimal order_price { get; set; }
        public string address { get; set; }
    }
}