using Hwdtech;
using Hwdtech.Ioc;
using Moq;

namespace BattleSpace.Lib.Test;
    public class StartMoveCommandTest {
        public StartMoveCommandTest() {
            new InitScopeBasedIoCImplementationCommand().Execute();

            IoC.Resolve<Hwdtech.ICommand>("Scopes.Current.Set", IoC.Resolve<object>("Scopes.New", IoC.Resolve<object>("Scopes.Root"))).Execute();

            var MCommand = new Mock<ICommand>();
            MCommand.Setup(m => m.Execute());
            var RStrategy = new Mock<IStrategy>();
            RStrategy.Setup(m => m.ExecuteStrategy(It.IsAny<object[]>())).Returns(MCommand.Object);

            IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Object.SetProperty", (object[] args) => RStrategy.Object.ExecuteStrategy(args)).Execute();
            IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Command.Move", (object[] args) => RStrategy.Object.ExecuteStrategy(args)).Execute();
            IoC.Resolve<Hwdtech.ICommand>("IoC.Register", "Queue.Push", (object[] args) => RStrategy.Object.ExecuteStrategy(args)).Execute();
        }

        [Fact]
        public void StartMoveCommandPosition() {
            var move_startable = new Mock<IMoveCommandStartable>();

            move_startable.SetupGet(m => m.velocity).Returns(new Vector(1, 1)).Verifiable();
            move_startable.SetupGet(m => m.uObject).Returns(new Mock<IUObject>().Object).Verifiable();
            move_startable.SetupGet(m => m.queue).Returns(new Mock<Queue<ICommand>>().Object).Verifiable();
            ICommand startMoveCommand = new StartMoveCommand(move_startable.Object);
            startMoveCommand.Execute();
            move_startable.Verify();
        }

        [Fact]
        public void StartMoveCommandUnreadableQueue() {
            var move_startable = new Mock<IMoveCommandStartable>();

            move_startable.SetupGet(m => m.velocity).Returns(new Vector(1, 1)).Verifiable();
            move_startable.SetupGet(m => m.uObject).Returns(new Mock<IUObject>().Object).Verifiable();
            move_startable.SetupGet(m => m.queue).Throws(new Exception()).Verifiable();
            ICommand startMoveCommand = new StartMoveCommand(move_startable.Object);
            Assert.Throws<Exception>(() => startMoveCommand.Execute());
        }
        
        [Fact]
        public void StartMoveCommandUnreadableObject() {
            var move_startable = new Mock<IMoveCommandStartable>();

            move_startable.SetupGet(m => m.velocity).Returns(new Vector(1, 1)).Verifiable();
            move_startable.SetupGet(m => m.uObject).Throws(new Exception()).Verifiable();
            move_startable.SetupGet(m => m.queue).Returns(new Mock<Queue<ICommand>>().Object).Verifiable();
            ICommand startMoveCommand = new StartMoveCommand(move_startable.Object);
            Assert.Throws<Exception>(() => startMoveCommand.Execute());
        }

        [Fact]
        public void StartMoveCommandUnreadableVelocity() {
            var move_startable = new Mock<IMoveCommandStartable>();

            move_startable.SetupGet(m => m.velocity).Throws(new Exception()).Verifiable();
            move_startable.SetupGet(m => m.uObject).Returns(new Mock<IUObject>().Object).Verifiable();
            move_startable.SetupGet(m => m.queue).Returns(new Mock<Queue<ICommand>>().Object).Verifiable();
            ICommand startMoveCommand = new StartMoveCommand(move_startable.Object);
            Assert.Throws<Exception>(() => startMoveCommand.Execute());
        }
    }