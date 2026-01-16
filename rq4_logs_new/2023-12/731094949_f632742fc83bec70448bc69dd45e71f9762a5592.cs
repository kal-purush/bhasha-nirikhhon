using QRisto.Application.Models.Request.Reservation;
using QRisto.Application.Models.Response.Reservation;
using QRisto.Application.Utils;
using QRisto.Persistence.Entity.Provider;
using QRisto.Persistence.Repositories.Implementations;

namespace QRisto.Application.Services.Reservation;

public class ReservationService : IReservationService
{
    private readonly UnitOfWork _unitOfWork;

    public ReservationService(UnitOfWork unitOfWork)
    {
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<List<ReservationRangeResponseModel>>> GetAvailableReservationsAsync(
        GetAvailableReservationsRequestModel model)
    {
        try
        {
            var operatingSchedule =
                await _unitOfWork.OperatingScheduleRepository.GetOperatingScheduleForDayAsync(model.Day);

            if (operatingSchedule is not { IsWorkingDay: true })
            {
                return Result<List<ReservationRangeResponseModel>>.Success(new List<ReservationRangeResponseModel>());
            }

            var reservations =
                await _unitOfWork.ReservationRepository.GetReservationsForDayAsync(model.ServiceId, model.Day);

            reservations ??= new List<ReservationEntity>();

            var availableSlots = CalculateAvailableSlots(
                model.Day,
                operatingSchedule.WorkingIntervals.ToList(),
                reservations);

            return Result<List<ReservationRangeResponseModel>>.Success(availableSlots);
        }
        catch (Exception ex)
        {
            return Result<List<ReservationRangeResponseModel>>.Failure(ex.ToString());
        }
    }

    private List<ReservationRangeResponseModel> CalculateAvailableSlots(
        DateOnly day,
        List<WorkingIntervalEntity> workingIntervals,
        List<ReservationEntity> reservations)
    {
        var availableSlots = new List<ReservationRangeResponseModel>();
        var dayStart = day.ToDateTime(new TimeOnly());

        foreach (var interval in workingIntervals)
        {
            var intervalStart = dayStart + interval.StartTime.ToTimeSpan();
            var intervalEnd = dayStart + interval.EndTime.ToTimeSpan();
            availableSlots.AddRange(FindAvailableSlotsInInterval(intervalStart, intervalEnd, reservations));
        }

        return availableSlots;
    }

    private IEnumerable<ReservationRangeResponseModel> FindAvailableSlotsInInterval(DateTime intervalStart,
        DateTime intervalEnd,
        IEnumerable<ReservationEntity> reservations)
    {
        var slots = new List<ReservationRangeResponseModel>();
        var lastEnd = intervalStart;

        var reservationsInInterval = reservations
            .Where(r => r.ReservationTime >= intervalStart && r.ReservationTime < intervalEnd)
            .OrderBy(r => r.ReservationTime);

        foreach (var reservation in reservationsInInterval)
        {
            if (lastEnd < reservation.ReservationTime)
            {
                slots.Add(new ReservationRangeResponseModel { Start = lastEnd, End = reservation.ReservationTime });
            }

            lastEnd = reservation.ReservationTime.AddMinutes(reservation.DurationInMinutes);
        }

        if (lastEnd < intervalEnd)
        {
            slots.Add(new ReservationRangeResponseModel { Start = lastEnd, End = intervalEnd });
        }

        return slots;
    }
}