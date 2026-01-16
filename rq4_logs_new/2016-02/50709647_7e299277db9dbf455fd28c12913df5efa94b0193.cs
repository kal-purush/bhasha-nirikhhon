using Microsoft.VisualStudio.TestTools.UnitTesting;
using MineSearch.Common;
using MineSearch.Wpf.Models;
using MineSearch.Wpf.ViewModels;
using Moq;

namespace MineSearch.Wpf.Test.ViewModelTests
{
    [TestClass]
    public class MainWindowViewModelTest
    {
        [TestMethod]
        public void TestExitCommand()
        {
            var window = new Mock<IBaseWindow>();
            window.Setup(w => w.Close());

            var vm = new MainWindowViewModel(new RandomPointGenerator());
            vm.ExitCommand.Execute(window.Object);

            window.Verify(w => w.Close(), Times.Once);
        }
    }
}