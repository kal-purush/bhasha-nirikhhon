namespace seaway.API.Models.ViewModels
{
    public class OfferWithPicModel
    {
        public string? Name { get; set; }
        public string? Description { get; set; }
        public double? Price { get; set; }
        public double? DiscountPercentage { get; set; }
        public DateTime? ValidFrom { get; set; }
        public DateTime? ValidTo { get; set; }
        public bool? IsRoomOffer { get; set; }
        public OfferPics[]? offerPics { get; set; }
    }

    public class OfferPics
    {
        public string? PicName { get; set; }
        public string? PicValue { get; set; }
        public string? CloudinaryPublicId { get; set; }
    }
}