using HaloBiz.Model;

namespace HaloBiz.DTOs.TransferDTOs
{
    public class OfficeTransferDTO
    {
        public long Id { get; set; }        
        public string Name { get; set; }
        public string Description { get; set; }
        public StateWithoutLGATransferDto State { get; set; }
        public LGATransferDTO LGA { get; set; }
        public string Street { get; set; }
        public string PhoneNumber { get; set; }
        public UserProfileTransferDTO Head { get; set; }

    }
}