using System;
using System.Windows;
using FusionMVVM.Service;
using FusionMVVM.Tests.Fakes;
using Xunit;
using Xunit.Extensions;

namespace FusionMVVM.Tests.Service.WindowLocatorTests
{
    public class CreateWindowTests
    {
        [Theory, CustomAutoData]
        public void NullViewModelThrowsException(WindowLocator sut, FakeViewModel owner)
        {
            Assert.Throws<ArgumentNullException>(() => sut.CreateWindow(null, owner));
        }

        [Theory, CustomAutoData]
        public void WindowHasCorrectDataContext(WindowLocator sut, FakeViewModel viewModel)
        {
            sut.Register(viewModel.GetType(), typeof(Window));
            var actual = sut.CreateWindow(viewModel, null);
            Assert.Same(viewModel, actual.DataContext);
        }
    }
}