using System.Linq;
using Auth.FWT.Core.Data;
using Auth.FWT.Domain.Entities.API;
using InquirerCS;
using static Auth.FWT.Domain.Enums.Enum;

namespace Auth.Manage
{
    public class Application : IApplication
    {
        private IUnitOfWork _unitOfWork;

        public Application(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public void Run()
        {
            Menu();
        }

        public void Menu()
        {
            Question.Menu()
                .AddOption("Create New Client", () => CreateNewClient())
                .AddOption("Set Client Status", () => SetClientActiveStatus())
            .Prompt();
        }

        private void SetClientActiveStatus()
        {
            var clients = _unitOfWork.ClientAPIRepository.GetAllIncluding().ToList();

            Question.Checkbox("Alter", clients)
            .Page(10)
            .WithDefaultValue(item => { return clients.Where(c => c.IsActive).Any(c => c.Id == item.Id); })
            .WithConfirmation()
            .WithConvertToString(item => { return $"{item.Name}"; }).Then(answer =>
            {
                var toDiactivate = clients.Where(c => c.IsActive).Where(c => !answer.Any(a => a.Id == c.Id)).ToList();
                toDiactivate.ForEach(item =>
                {
                    item.IsActive = false;
                    _unitOfWork.ClientAPIRepository.Update(item);
                });

                var toActivate = clients.Where(c => !c.IsActive).Where(c => answer.Any(a => a.Id == c.Id)).ToList();
                toActivate.ForEach(item =>
                {
                    item.IsActive = true;
                    _unitOfWork.ClientAPIRepository.Update(item);
                });

                _unitOfWork.SaveChanges();
            });
        }

        private void CreateNewClient()
        {
            var client = new ClientAPI();

            Question.Ask()
            .Then(() => { Question.Input("Id").Then(answer => client.Id = answer); })
            .Then(() => { Question.Input("Name").WithDefaultValue(client.Id).Then(answer => client.Name = answer); })
            .Then(() => { Question.Input("Allowed Origin").WithDefaultValue("*").Then(answer => client.AllowedOrigin = answer); })
            .Then(() => { Question.Input<int>("Refresh Token Lifetime (hours)").WithValidation(answer => answer > 0, "answer > 0").Then(answer => client.RefreshTokenLifeTime = answer * 60); })
            .Then(() => { Question.Confirm("Is Active").Then(answer => client.IsActive = answer); })
            .Then(() =>
            {
                client.ApplicationType = ApplicationType.JavaScript;
                client.Secret = "";

                _unitOfWork.ClientAPIRepository.Insert(client);
                _unitOfWork.SaveChanges();
            })
            .Go();

            SetClientActiveStatus();
        }
    }
}