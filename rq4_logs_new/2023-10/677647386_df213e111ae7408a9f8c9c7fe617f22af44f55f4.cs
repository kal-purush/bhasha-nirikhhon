using TsdDelivery.Application.Models;
using TsdDelivery.Application.Models.Reservation.Request;
using TsdDelivery.Application.Services.Momo.Request;
using TsdDelivery.Domain.Entities;

namespace TsdDelivery.Application.Interface;

public interface IPayPalService
{
    public Task<(bool, string?, string?)> CreatePaypalPayment(CreateReservationRequest request,Reservation reservation);
    //public Task<OperationResult<string>> ProcessPayPalPaymentReturn(MomoOneTimePaymentResultRequest paymentResultRequest);
    public Task<string> GenerateAccessToken(string clientId, string clientSecret, string paypalUrl);
}    