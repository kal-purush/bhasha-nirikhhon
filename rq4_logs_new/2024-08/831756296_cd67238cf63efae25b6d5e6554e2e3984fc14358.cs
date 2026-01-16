using Application.ViewModels;
using Bogus;
using Domain.Entities;

namespace Tests.Fixtures
{
    public static class ContactFixture
    {

        public static Contact CreateFakeContact()
        {
            var contact = new Faker<Contact>()
               .RuleFor(c => c.Id, f => f.Random.Int())
               .RuleFor(c => c.CreatedAt, f => f.Date.Recent())
               .RuleFor(c => c.Name, f => f.Person.FullName)
               .RuleFor(c => c.Email, f => f.Person.Email)
               .RuleFor(c => c.Ddd, f => f.Random.Number(11, 99).ToString())
               .RuleFor(c => c.Phone, f => f.Person.Phone)
               .Generate();

            return contact;
        }

        public static ContactRequestModel CreateFakeContactRequestModel()
        {
            var contact = new Faker<ContactRequestModel>()
               .RuleFor(c => c.Name, f => f.Person.FullName)
               .RuleFor(c => c.Email, f => f.Person.Email)
               .RuleFor(c => c.Ddd, f => f.Random.Number(11, 99).ToString())
               .RuleFor(c => c.Phone, f => f.Person.Phone)
               .Generate();

            return contact;
        }
    }
}