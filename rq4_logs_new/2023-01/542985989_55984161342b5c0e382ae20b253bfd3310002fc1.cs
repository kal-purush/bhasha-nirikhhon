using Hwdtech;
using Hwdtech.Ioc;
using Moq;

namespace BattleSpace.Lib.Test;

public class EndMoveCommandTests {
    public EndMoveCommandTests() {
        new InitScopeBasedIoCImplementationCommand().Execute();
        IoC.Resolve<Hwdtech.ICommand>("Scopes.Current.Set", IoC.Resolve<object>("Scopes.New", IoC.Resolve<object>("Scopes.Root"))).Execute();
        var CommandMock = new Mock<ICommand>();
        var regStrategy = new Mock<IStrategy>();
        var GetObj = new Mock<IStrategy>();
        regStrategy.Setup(m => m.ExecuteStrategy(It.IsAny<object[]>())).Returns(CommandMock.Object);
        GetObj.Setup(m => m.ExecuteStrategy(It.IsAny<object[]>())).Returns(CommandMock.Object);
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Command.EmptyCommand", (object[] args) => regStrategy.Object.ExecuteStrategy(args)).Execute();
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Game.DeleteProperty", (object[] args) => regStrategy.Object.ExecuteStrategy(args)).Execute();
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Game.InjectCommand", (object[] args) => regStrategy.Object.ExecuteStrategy(args)).Execute();
        IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Game.MyObject.GetUObj", (object[] args) => regStrategy.Object.ExecuteStrategy(args)).Execute();
    }
    [Fact]
    public void EndMoveCommandPos() {
        var moveendable = new Mock<IMoveCommandEndable>();
     
        moveendable.SetupGet(m => m.UObject).Returns(new Mock<IUObject>().Object);
        moveendable.SetupGet(m => m.Queue).Returns(new Mock<Queue<ICommand>>().Object);
        ICommand EndMoveCommand = new EndMoveCommand(moveendable.Object);
        EndMoveCommand.Execute();
        moveendable.VerifyAll();
    }

    [Fact]
    public void EndMoveCommandUnreadableObject() {
        var moveendable = new Mock<IMoveCommandEndable>();

        moveendable.SetupGet(m => m.UObject).Throws(new Exception());
        moveendable.SetupGet(m => m.Queue).Returns(new Mock<Queue<ICommand>>().Object);
        ICommand endMoveCommand = new EndMoveCommand(moveendable.Object);
        Assert.Throws<Exception>(() => endMoveCommand.Execute());
    }

    [Fact]
    public void EndMoveCommandUnreadableQueue() {
        var moveendable = new Mock<IMoveCommandEndable>();

        moveendable.SetupGet(m => m.UObject).Returns(new Mock<IUObject>().Object);
        moveendable.SetupGet(m => m.Queue).Throws(new Exception());
        ICommand endMoveCommand = new EndMoveCommand(moveendable.Object);
        Assert.Throws<Exception>(() => endMoveCommand.Execute());
    }    
}