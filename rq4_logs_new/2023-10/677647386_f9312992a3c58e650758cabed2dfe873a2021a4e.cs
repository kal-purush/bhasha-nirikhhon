using TsdDelivery.Application.Models.Reservation.DTO;
using TsdDelivery.Application.Models.User.DTO;

namespace TsdDelivery.Application.Models.Reservation.Response;

public class ReservationHistoryDetailResponse
{
    public Guid Id { get; set; }
    public string SendLocation { get; set; }
    public string ReciveLocation { get; set; }
    public string RecipientName { get; set; }
    public string RecipientPhone { get; set;}
    public GoodsDto GoodsDto { get; set; }
    public decimal Distance { get; set; }
    public bool IsNow { get; set; }
    public DateTime PickUpDateTime { get; set; }
    public decimal TotallPrice { get; set; }
    public string ReservationStatus { get; set; }
    public DriverDto? DriverDto { get; set; }
}