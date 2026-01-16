using AskHand.Domain.Cities;
using AskHand.Domain.Countries;
using AskHand.Domain.Emails;
using AskHand.Domain.Passwords;
using AskHand.Domain.Skills;

namespace AskHand.Domain.Users;

public sealed class User : AggregateRoot<UserId>
{
    public Email Email { get; }

    public Password Password { get; }

    public UserMetaData MetaData { get; }

    public Skill Skill { get; }

    public City City { get; }

    public Country Country { get; }

    private User(
        UserId id,
        Email email,
        Password password,
        UserMetaData metaData,
        Skill skill,
        City city,
        Country country) 
        : base(id)
    {
        Email = email;
        Password = password;
        MetaData = metaData;
        Skill = skill;
        City = city;
        Country = country;
        MetaData = metaData;
    }

    public static User Create(
        Email email,
        Password password,
        UserMetaData metaData,
        Skill skill,
        City city,
        Country country)
    {
        return new User(
            UserId.CreateUnique(),
            Email.Create(email.Value), 
            Password.Create(password.Value),
            metaData,
            Skill.Create(skill.Level), 
            City.Create(city.Name), 
            Country.Create(country.Name));
    }
}