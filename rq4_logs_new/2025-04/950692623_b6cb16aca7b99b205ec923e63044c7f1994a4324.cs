namespace HotelReservation
{
    public partial class Function
    {
        public class AccountValidater
        {
            private Account? GetAccount(AccountVerificationType type, string input)
            {
                return type == AccountVerificationType.Email
                    ? AccountManager.Accounts?.Find(i => i.Email == input)
                    : AccountManager.Accounts?.Find(i => i.PhoneNumber == input);
            }
            private Account? GetAccount(UID id)
            {
                return AccountManager.Accounts?.Find(i => i.ID == id);
            }

            public void SignAccount(AccountSignType signType, AccountVerificationType verificationType, string input, string password, string? confirmedPassword = null)
            {
                Account? account = GetAccount(verificationType, input);

                if (verificationType == AccountVerificationType.Email)
                {
                    if (!Extension.Validator.ValidateEmail(input))
                    {
                        Event.OnAccountVerificated?.Invoke(signType == AccountSignType.SignUp
                            ? AccountVerificationResult.SignUpEmailNotValid
                            : AccountVerificationResult.SignInEmailNotValid);

                        return;
                    }
                }
                else if (verificationType == AccountVerificationType.PhoneNumber)
                {
                    if (!Extension.Validator.ValidatePhoneNumber(input))
                    {
                        Event.OnAccountVerificated?.Invoke(signType == AccountSignType.SignUp
                            ? AccountVerificationResult.SignUpPhoneNumberNotValid
                            : AccountVerificationResult.SignInPhoneNumberNotValid);

                        return;
                    }
                }

                if (signType == AccountSignType.SignUp)
                {
                    if (account != null)
                    {
                        Event.OnAccountVerificated?.Invoke(verificationType == AccountVerificationType.Email
                            ? AccountVerificationResult.SignUpEmailHasExisted
                            : AccountVerificationResult.SignUpPhoneNumberHasExisted);

                        return;
                    }
                    else if (password != confirmedPassword)
                    {
                        Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignUpPasswordNotMatch);

                        return;
                    }

                    Console.WriteLine("Signing up may pass!");
                }
                else if (signType == AccountSignType.SignIn)
                {
                    if (account == null)
                    {
                        Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignInAccountNotFound);

                        return;
                    }
                    else if (account.Password != password)
                    {
                        Event.OnAccountVerificated?.Invoke(AccountVerificationResult.SignInPasswordNotCorrect);

                        return;
                    }

                    Console.WriteLine($"Account ID: {account.ID}");
                }
            }

            public void DeleteAccount(UID id)
            {
                Account? account = GetAccount(id);

                if (account == null)
                {
                    Event.OnAccountDeleted?.Invoke(AccountDeletionResult.AccountNotFound);

                    return;
                }

                AccountManager.Accounts?.Remove(account);
                Event.OnAccountDeleted?.Invoke(AccountDeletionResult.AccountDeletionSuccess);

                Console.WriteLine($"Account with ID {id} has been deleted.");
            }
        }
    }
}