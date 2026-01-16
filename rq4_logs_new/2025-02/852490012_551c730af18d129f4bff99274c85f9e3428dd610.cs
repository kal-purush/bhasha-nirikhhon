using SupplyChainTesting.MockClasses;
using WPFTesting.Data;
using WPFTesting.Shapes;
using YourNamespace;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Windows;

[TestFixture]
[Apartment(ApartmentState.STA)] // Ensure STA for WPF components
public class MainWindowTests
{
    private MainWindow _testWindow;
    private IInitializedDataProvider _dataProvider;
    private SupplierElement _box;

    private IEnumerable<SupplierUIValues> _boxes;

    [SetUp]
    public void SetUp()
    {
        _dataProvider = new DataProvider_FAKE();
        _boxes = _dataProvider.GetBoxValues(); // Initialize _boxes here

        // Optionally, print the box details if necessary for debugging
        foreach (var box in _boxes)
        {
            Console.WriteLine($"Supplier Name: {box.supplier.Name}, Position: {box.Position}");
        }

        // Initialize your window if needed
        _testWindow = new MainWindow();
    }

    [Test]
    public void TestBoxValues()
    {
        Assert.IsNotNull(_boxes, "Box values should not be null.");
        Assert.IsTrue(_boxes.Any(), "No box values returned.");
    }

    [Test]
    [TestCase("Vendor 1", 140, 140, "NE_Radial", 140, 140, 1)]
    [TestCase("Vendor 2", 280, 280, "S_Radial", 280, 280,2 )]
    [TestCase("Vendor 3", 50, 320, "W_Radial", 50, 320,3)]
    public void GetLineOffset_ReturnsCorrectPoint(string boxName, double width, double height, string cornerClicked, double expectedX, double expectedY, int number)
    {
        // Arrange
        var supplierUIValues = _boxes.FirstOrDefault(s => s.supplier.Name == boxName);
        Assert.IsNotNull(supplierUIValues, $"Supplier with name '{boxName}' not found.");
        _boxes.ElementAt(number);

        // Act
        var actualPoint = _testWindow.GetLineOffset(_box);

        // Assert
        var expectedPoint = new Point(expectedX, expectedY);
        Assert.AreEqual(expectedPoint, actualPoint, $"Expected point ({expectedX}, {expectedY}) but got ({actualPoint.X}, {actualPoint.Y}).");
    }
}