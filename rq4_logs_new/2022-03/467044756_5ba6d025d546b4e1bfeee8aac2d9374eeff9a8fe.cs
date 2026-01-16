using HexMaster.Email.DomainModels;
using HexMaster.Email.Exceptions;
using Xunit;

namespace HexMaster.Email.UnitTests.DomainModels
{
    public class RecipientTests
    {

        [Fact]
        public void WhenRecipientHasDuplicateSubstitutions_ItThrows()
        {
            var recipient = new Recipient("email@domain.com");
            recipient.AddSubstitution("test", "value");
            var exception = Assert.Throws<SubstitutionAlreadyExistsException>(() => recipient.AddSubstitution("test", "other value"));
            Assert.Contains("test", exception.Message);
        }
    }
}