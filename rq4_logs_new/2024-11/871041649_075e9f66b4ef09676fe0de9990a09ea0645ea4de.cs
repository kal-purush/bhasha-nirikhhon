namespace backend.DTOs
{
    public class RestaurantsDto
    {
        public int OwnerId { get; set; }
        public string Name { get; set; }
        public string PhoneNumber { get; set; }
        public decimal? Rating { get; set; }
        public DateTime? OpeningTime { get; set; }
        public DateTime? ClosingTime { get; set; }
        public string? image_url { get; set; }
        public string StreetAddress { get; set; }
        public string AdditionalAddress { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Pincode { get; set; }
    }
}