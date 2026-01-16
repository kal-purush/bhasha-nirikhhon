using DomainDrivenDesign.List5_4;
using DomainDrivenDesign.List5_4.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace DomainDrivenDesignUnitTest;

public class UnitTest5_4
{
    [Test]
    public void DI()
    {
        var di = DependencyInjection.Services;

        var user1 = di.GetService<IUserNameFactory>()?.Create("Garaoh70");
        var user2 = di.GetService<IUserNameFactory>()?.Create("Garaoh70");

        // DIで作成したオブジェクトを比較
        Assert.That(user1, Is.EqualTo(user2));
    }
}
