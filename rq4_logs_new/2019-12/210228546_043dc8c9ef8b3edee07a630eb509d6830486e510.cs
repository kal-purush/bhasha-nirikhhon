namespace Carter.App.Validation.Person
{
    using FluentValidation;

    public class Person
    {
        #pragma warning disable SA1516, SA1300
        public int id { get; set; }
        public string firstname { get; set; }
        public string lastname { get; set; }
        public string fullname { get; set; }
        public string email { get; set; }
        public string idToken { get; set; }
        public string signUpToken { get; set; }
        #pragma warning restore SA151, SA1300
    }

    public class PersonValidator : AbstractValidator<Person>
    {
        public PersonValidator()
        {
            this.RuleFor(x => x.id).NotEmpty();
            this.RuleFor(x => x.firstname).NotEmpty();
            this.RuleFor(x => x.lastname).NotEmpty();
            this.RuleFor(x => x.fullname).NotEmpty();
            this.RuleFor(x => x.email).NotEmpty();
            this.RuleFor(x => x.idToken).NotEmpty();
            this.RuleFor(x => x.signUpToken).NotEmpty();
        }
    }
}