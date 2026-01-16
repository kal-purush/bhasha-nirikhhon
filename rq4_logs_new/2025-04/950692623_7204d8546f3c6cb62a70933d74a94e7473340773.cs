namespace HotelReservation
{
    public class AccountManager
    {
        public Account Account = new();

        public void SignUpAccount(AccountVerificationType type, string account, string password)
        {
            if (type == AccountVerificationType.Email)
            {
                if (!Handler.Validator.ValidateEmail(account))
                {
                    Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignUpEmailNotValid);
                    return;
                }
                if (true) // Check if email has existed in database
                {
                    Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignUpEmailHasExisted);
                    return;
                }

                // Try get account ID from database
            }
            else if (type == AccountVerificationType.PhoneNumber)
            {
                if (!Handler.Validator.ValidatePhoneNumber(account))
                {
                    Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignUpPhoneNumberNotValid);
                    return;
                }
                if (true) // Check if phone number has existed in database
                {
                    Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignUpPhoneNumberHasExisted);
                    return;
                }

                // Try get account ID from database
            }
        }

        public void AssignAccount(UID id)
        {

        }
    }
}