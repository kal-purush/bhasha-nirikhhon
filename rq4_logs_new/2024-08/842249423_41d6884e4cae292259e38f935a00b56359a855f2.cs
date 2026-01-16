using TJC.MVVM.Tests.Mocks;

namespace TJC.MVVM.Tests.Refreshing;

[TestClass]
public class ModelToViewModelConverterTests
{
    [TestMethod]
    public void RefreshingModelRefreshesViewModel()
    {
        var model = new ModelMock();
        var viewModel = new ViewModelMock(model);
        Assert.AreEqual(1, viewModel.RefreshCount, "One Refresh Occurs on Creation");
        model.RunRefresh();
        Assert.AreEqual(2, viewModel.RefreshCount, "Another Refresh Occurs when the Model Refreshes");
    }
}