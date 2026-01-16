using Machine.Specifications;
using Rhino.Mocks;
using Rhino.Mspec.Contrib;
using zavit.Domain.Accounts.Registrations;
using zavit.Domain.Accounts.Registrations.Validators;

namespace zavit.Domain.Accounts.Tests.Registrations.Validators
{
    [Subject("DisplayNameValidator")]
    public class DisplayNameValidatorTests : TestOf<DisplayNameValidator>
    {
        class When_validating_account_registration_display_name_that_is_not_blank
        {
            Because of = () => _result = Subject.Validate(_accountRegistration);

            It should_return_null = () => _result.ShouldBeNull();

            Establish context = () =>
            {
                _accountRegistration = NewInstanceOf<IAccountRegistration>();
                _accountRegistration.Stub(r => r.DisplayName).Return("Test display name");
            };

            static IAccountRegistration _accountRegistration;
            static AccountRegistrationResult _result;
        }

        class When_validating_account_registration_display_name_that_is_blank
        {
            Because of = () => _result = Subject.Validate(_accountRegistration);

            It should_return_failed_result = () => _result.ShouldEqual(_accountRegistrationResult);

            Establish context = () =>
            {
                _accountRegistration = NewInstanceOf<IAccountRegistration>();
                _accountRegistration.Stub(r => r.DisplayName).Return("");

                _accountRegistrationResult = NewInstanceOf<AccountRegistrationResult>();
                Injected<IAccountRegistrationResultFactory>()
                    .Stub(v => v.CreateFailed(DisplayNameValidator.DisplayNameBlankMessage))
                    .Return(_accountRegistrationResult);
            };

            static IAccountRegistration _accountRegistration;
            static AccountRegistrationResult _result;
            static AccountRegistrationResult _accountRegistrationResult;
        }
    }
}