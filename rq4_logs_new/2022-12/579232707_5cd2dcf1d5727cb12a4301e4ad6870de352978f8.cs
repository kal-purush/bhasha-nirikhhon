namespace Catcheap.API.Interfaces.IService.IMiscServices;

public interface IValidationService
{

    bool ValidateNotNull(string input);

    bool ValidateNumber(string input);

    bool ValidateDate(string input);

    bool ValidateNumberMoreThanZero(double number);

    bool ValidateNumberMoreThanOrEqualZero(double number);

    bool ValidatePercentage(double number);

    bool ValidateDateNowOrMore(DateTime date);

    bool ValidateYearNowOrMore(int number);

    bool ValidateDatesLaterOrEqualToOther(DateTime dateEarlier, DateTime dateLater);

}