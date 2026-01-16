using SFA.DAS.Funding.ApprenticeshipPayments.Domain.Models;
using SFA.DAS.Funding.ApprenticeshipPayments.Infrastructure.Api.Requests;
using SFA.DAS.Funding.ApprenticeshipPayments.Infrastructure.Api.Responses;
using SFA.DAS.Funding.ApprenticeshipPayments.Infrastructure.Interfaces;

namespace SFA.DAS.Funding.ApprenticeshipPayments.Command;

internal static class OuterApiExtensions
{
    internal static async Task<AcademicYears> GetAcademicYearsDetails(this IOuterApiClient outerApiClient, DateTime now)
    {
        var currentAcademicYearResponse = await outerApiClient.Get<GetAcademicYearsResponse>(new GetAcademicYearsRequest(now));
        if (!currentAcademicYearResponse.IsSuccessStatusCode || currentAcademicYearResponse.Body == null)
            throw new Exception("Error getting current academic year");

        var lastDayOfPreviousYear = currentAcademicYearResponse.Body.StartDate.AddDays(-1);

        var previousAcademicYearResponse = await outerApiClient.Get<GetAcademicYearsResponse>(new GetAcademicYearsRequest(lastDayOfPreviousYear));
        if (!previousAcademicYearResponse.IsSuccessStatusCode || previousAcademicYearResponse.Body == null)
            throw new Exception("Error getting previous academic year");

        return new AcademicYears(previousAcademicYearResponse.Body.Parse(), currentAcademicYearResponse.Body.Parse());
    }

    private static AcademicYearDetails Parse(this GetAcademicYearsResponse academicYearsResponse)
    {
        try
        {
            return new AcademicYearDetails(
                short.Parse(academicYearsResponse.AcademicYear),
                academicYearsResponse.StartDate,
                academicYearsResponse.EndDate,
                academicYearsResponse.HardCloseDate
            );
        }
        catch (Exception ex)
        {
            throw new Exception($"Error parsing academic year response to AcademicYearDetails: {ex.Message}", ex);
        }
    }
}