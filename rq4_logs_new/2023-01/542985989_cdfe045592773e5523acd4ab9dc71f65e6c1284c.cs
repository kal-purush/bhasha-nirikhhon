using Hwdtech;
using Hwdtech.Ioc;
using Moq;

namespace BattleSpace.Lib.Test;

public class ConstructingATreeTest
{

    public ConstructingATreeTest()
    {
        new ImplementationIoCCommand().Execute();
        IoC.Resolve<Hwdtech.ICommand>("Scopes.Current.Set", IoC.Resolve<object>("Scopes.New", IoC.Resolve<object>("Scopes.Root"))).Execute();
    }

    [Fact]
    public void PositiveConstructingATreeTest()
    {
        string way = "../../../../SpaceBattle.Lib/Data.txt";
        var getTreeStrategy = new Mock<IStrategy>();
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Game.ConstructingATree", (object[] args) => getTreeStrategy.Object.ExecuteStrategy(args)).Execute();
        getTreeStrategy.Setup(t => t.ExecuteStrategy(It.IsAny<object[]>())).Returns(new Dictionary<int, object>()).Verifiable();

        var bdt = new ConstructingATree(way);

        bdt.Execute();

        getTreeStrategy.Verify();
    }

    [Fact]
    public void NegativeConstructingATreeTestThrowsException()
    {
        string way = "";
        var getTreeStrategy = new Mock<IStrategy>();
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Game.ConstructingATree", (object[] args) => getTreeStrategy.Object.ExecuteStrategy(args)).Execute();
        getTreeStrategy.Setup(t => t.ExecuteStrategy(It.IsAny<object[]>())).Returns(new Dictionary<int, object>()).Verifiable();

        var bdt = new ConstructingATree(way);

        Assert.Throws<Exception>(() => bdt.Execute());

        getTreeStrategy.Verify();
    }

    [Fact]
    public void FileNotFoundException()
    {
        string way = "./.txt";
        var getTreeStrategy = new Mock<IStrategy>();
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Game.ConstructingATree", (object[] args) => getTreeStrategy.Object.ExecuteStrategy(args)).Execute();
        getTreeStrategy.Setup(t => t.ExecuteStrategy(It.IsAny<object[]>())).Returns(new Dictionary<int, object>()).Verifiable();

        var bdt = new ConstructingATree(way);

        Assert.Throws<FileNotFoundException>(() => bdt.Execute());

        getTreeStrategy.Verify();
    }
}