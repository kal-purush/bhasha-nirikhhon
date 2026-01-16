using System.Collections.ObjectModel;
using System.Drawing;
using SupplyChainTesting.MockClasses;
using WPFTesting.Models;
using WPFTesting.Shapes;

namespace DataProviderFakeTests;
[TestFixture]
public class DataProviderFakeTests
{
    [Test]
    public void GetBoxValues_ShouldReturnCorrectNumberOfSuppliers()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        Assert.IsNotNull(suppliers);
        Assert.AreEqual(3, suppliers.Count());
    }

    [Test]
    public void GetShipments_ShouldReturnExpectedShipmentData()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpointNode = RealEnpointForTest.makeAnEnpointForTest();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = endpointNode }
        };

        var suppliers = dataProvider.GetBoxValues();
        var shipments = dataProvider.GetShipments(endpoints, suppliers);

        Assert.IsNotEmpty(shipments);
        var shipment = shipments.First();

        Assert.AreEqual("wood", shipment.Products.First().ProductName);
        Assert.AreEqual("Vendor 3", shipment.Sender.Name);
        Assert.AreEqual("Vendor 2", shipment.Receiver.Name);
    }

    [Test]
    public void SaveShipmentInfo_ShouldNotThrowExceptions()
    {
        var dataProvider = new DataProvider_FAKE();
        var shipments = new List<Shipment>
        {
            new Shipment
            {
                Id = System.Guid.NewGuid(),
                Products = new List<Product>
                {
                    new Product { ProductName = "Sample Product", Quantity = 5 }
                },
                Sender = new Supplier { Name = "Sender" },
                Receiver = new Supplier { Name = "Receiver" }
            }
        };

        Assert.DoesNotThrow(() => dataProvider.SaveShipmentInfo(shipments));
    }

    [Test]
    public void RealEndpointForTest_ShouldCreateCorrectEndpoint()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        Assert.IsNotNull(endpoint);
        Assert.AreEqual("Factory", endpoint.Name);
        Assert.AreEqual(1, endpoint.ProductInventory.Count);
        Assert.AreEqual(30, endpoint.ComponentInventory.First(p => p.ProductName == "wood").Quantity);
    }




    [Test]
    public void GetBoxValues_ShouldReturnExpectedNumberOfSuppliers()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        Assert.AreEqual(3, suppliers.Count(), "Expected 3 suppliers in the collection.");
    }

    [Test]
    public void GetBoxValues_SupplierPositions_ShouldBeCorrect()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues().ToList();

        Assert.AreEqual(new Point(140, 140), suppliers[0].Position);
        Assert.AreEqual(new Point(280, 280), suppliers[1].Position);
        Assert.AreEqual(new Point(50, 320), suppliers[2].Position);
    }

    [Test]
    public void GetShipments_ShouldReturnOneShipmentInVersion3()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = new EndpointNode() }
        };
        var suppliers = dataProvider.GetBoxValues();

        var shipments = dataProvider.GetShipments(endpoints, suppliers);

        Assert.AreEqual(1, shipments.Count(), "Expected exactly one shipment.");
        Assert.AreEqual("wood", shipments.First().Products.First().ProductName);
        Assert.AreEqual(10, shipments.First().Products.First().Quantity);
    }

    [Test]
    public void ProductionList_ShouldBeEnabledForProductLine()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        var productLine = endpoint.ProductionList.First();

        Assert.IsTrue(productLine.IsEnabled, "Production line should be enabled by default.");
    }

    [Test]
    public void Shipment_SenderAndReceiverInventories_ShouldHaveCorrectProducts()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = RealEnpointForTest.makeAnEnpointForTest() }
        };
        var suppliers = dataProvider.GetBoxValues();
        var shipment = dataProvider.GetShipments(endpoints, suppliers).First();

        Assert.AreEqual(4, shipment.Sender.ProductInventory.Count, "Sender should have 4 inventory products.");
        Assert.AreEqual(2, shipment.Receiver.ProductInventory.Count, "Receiver should have 2 inventory products.");
    }

    [Test]
    public void EndpointNode_ProductionList_ShouldProduceCorrectProduct()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        var productLine = endpoint.ProductionList.First();
        var resultingProduct = productLine.ResultingProduct;

        Assert.AreEqual("box", resultingProduct.ProductName);
        Assert.AreEqual(1, resultingProduct.Quantity);
    }

    [Test]
    public void SaveShipmentInfo_ShouldNotThrowException()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var shipments = new List<Shipment>
        {
            new Shipment
            {
                Products = new List<Product>
                {
                    new Product { ProductName = "wood", Quantity = 5 }
                },
                Sender = new Supplier { Name = "Vendor 1" },
                Receiver = new Supplier { Name = "Vendor 2" }
            }
        };

        Assert.DoesNotThrow(() => dataProvider.SaveShipmentInfo(shipments));
    }
    [Test]
    public void GetBoxValues_ShouldContainCorrectSupplierNames()
    {
        var dataProvider = new DataProvider_FAKE_Version2();
        var suppliers = dataProvider.GetBoxValues().ToList();

        Assert.Contains("Vendor 1", suppliers.Select(s => s.supplier.Name).ToList());
        Assert.Contains("Vendor 2", suppliers.Select(s => s.supplier.Name).ToList());
        Assert.Contains("Vendor 3", suppliers.Select(s => s.supplier.Name).ToList());
    }

    [Test]
    public void GetShipments_WhenNoShipmentsExist_ShouldReturnEmptyList()
    {
        var dataProvider = new DataProvider_FAKE();
        var endpoints = new List<EndpointUIValues>();
        var suppliers = dataProvider.GetBoxValues();

        var shipments = dataProvider.GetShipments(endpoints, suppliers);

        Assert.IsEmpty(shipments);
    }

    [Test]
    public void SaveSupplierInfo_ShouldNotThrowExceptions()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        Assert.DoesNotThrow(() => dataProvider.SaveSupplierInfo(suppliers));
    }

    [Test]
    public void ProductionList_ShouldContainCorrectComponentsForProductLine()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();
        var productLine = endpoint.ProductionList.First();

        Assert.AreEqual("box", productLine.ResultingProduct.ProductName);
        Assert.AreEqual(2, productLine.Components.Count);
        Assert.IsTrue(productLine.Components.Any(c => c.ProductName == "wood" && c.Quantity == 6));
        Assert.IsTrue(productLine.Components.Any(c => c.ProductName == "glue" && c.Quantity == 2));
    }

    [Test]
    public void EndpointNode_InventoryShouldHaveCorrectInitialValues()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        Assert.AreEqual(30, endpoint.ComponentInventory.First(p => p.ProductName == "wood").Quantity);
        Assert.AreEqual(10, endpoint.ComponentInventory.First(p => p.ProductName == "glue").Quantity);
        Assert.AreEqual(400, endpoint.ComponentInventory.First(p => p.ProductName == "nails").Quantity);

        Assert.AreEqual(0, endpoint.ProductInventory.First(p => p.ProductName == "box").Quantity);
    }

    [Test]
    public void Shipment_ShouldCorrectlyTrackSenderReceiver()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpointNode = RealEnpointForTest.makeAnEnpointForTest();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = endpointNode }
        };
        var suppliers = dataProvider.GetBoxValues();
        var shipments = dataProvider.GetShipments(endpoints, suppliers);

        var shipment = shipments.First();

        Assert.AreEqual("Vendor 3", shipment.Sender.Name);
        Assert.AreEqual("Vendor 2", shipment.Receiver.Name);
        Assert.AreEqual("wood", shipment.Products.First().ProductName);
    }
    [Test]
    public void GetBoxValues_ShouldReturnSuppliersWithUniqueNames()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        var uniqueNames = suppliers.Select(s => s.supplier).Distinct();

        Assert.AreEqual(suppliers.Count(), uniqueNames.Count(), "Supplier names should be unique.");
    }

    [Test]
    public void ProductionList_ShouldHaveNonEmptyResultingProducts()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        foreach (var productLine in endpoint.ProductionList)
        {
            Assert.IsNotNull(productLine.ResultingProduct, "Resulting product should not be null.");
            Assert.IsFalse(string.IsNullOrWhiteSpace(productLine.ResultingProduct.ProductName),
                "Resulting product should have a valid name.");
        }
    }

    [Test]
    public void ShipmentProductQuantities_ShouldBeAccurate()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = RealEnpointForTest.makeAnEnpointForTest() }
        };
        var suppliers = dataProvider.GetBoxValues();
        var shipment = dataProvider.GetShipments(endpoints, suppliers).First();

        Assert.AreEqual(10, shipment.Products.First().Quantity, "Shipment product quantity should be accurate.");
    }

    [Test]
    public void SaveShipmentInfo_ShouldSaveAllShipmentsCorrectly()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var shipments = new List<Shipment>
        {
            new Shipment
            {
                Products = new List<Product>
                {
                    new Product { ProductName = "wood", Quantity = 8 },
                    new Product { ProductName = "metal", Quantity = 5 }
                },
                Sender = new Supplier { Name = "Supplier X" },
                Receiver = new Supplier { Name = "Supplier Y" }
            }
        };

        Assert.DoesNotThrow(() => dataProvider.SaveShipmentInfo(shipments),
            "Saving shipment information should not throw exceptions.");
    }

    [Test]
    public void ProductionList_ShouldNotHaveDuplicateProducts()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        var productNames = endpoint.ProductionList.Select(pl => pl.ResultingProduct.ProductName);
        var uniqueProductNames = productNames.Distinct();

        Assert.AreEqual(productNames.Count(), uniqueProductNames.Count(), "Production list should not have duplicate products.");
    }

    [Test]
    public void GetBoxValues_ShouldIncludeSpecificProductInInventory()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        var supplier = suppliers.First();
        var product = supplier.supplier.ProductInventory.FirstOrDefault(p => p.ProductName == "Drill Bit");

        Assert.IsNotNull(product, "Expected 'Drill Bit' to be in supplier inventory.");
        Assert.AreEqual(20, product.Quantity, "Expected quantity of 'Drill Bit' to be 20.");
    }

    [Test]
    public void GetShipments_ShouldContainCorrectProductNames()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = RealEnpointForTest.makeAnEnpointForTest() }
        };
        var suppliers = dataProvider.GetBoxValues();
        var shipments = dataProvider.GetShipments(endpoints, suppliers);

        Assert.AreEqual("wood", shipments.First().Products.First().ProductName, "Expected 'wood' as the first product in the shipment.");
        Assert.AreEqual("nails", shipments.First().Products.Last().ProductName, "Expected 'nails' as the last product in the shipment.");
    }

    [Test]
    public void SaveSupplierInfo_ShouldNotThrowExceptionWhenSavingValidSuppliers()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = new List<SupplierUIValues>
        {
            new SupplierUIValues
            {
                supplier = new Supplier
                {
                    Name = "New Supplier",
                    ProductInventory = new ObservableCollection<Product>
                    {
                        new Product { ProductName = "New Product", Quantity = 10 }
                    }
                },
                Position = new Point(100, 100)
            }
        };

        Assert.DoesNotThrow(() => dataProvider.SaveSupplierInfo(suppliers), "Saving valid suppliers should not throw an exception.");
    }

    [Test]
    public void ShipmentProductQuantity_ShouldBeGreaterThanZero()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var endpoints = new List<EndpointUIValues>
        {
            new EndpointUIValues { supplier = RealEnpointForTest.makeAnEnpointForTest() }
        };
        var suppliers = dataProvider.GetBoxValues();
        var shipment = dataProvider.GetShipments(endpoints, suppliers).First();

        Assert.IsTrue(shipment.Products.All(p => p.Quantity > 0), "All shipment product quantities should be greater than zero.");
    }

    [Test]
    public void Endpoint_ShouldHaveNonEmptyComponentInventory()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        Assert.IsTrue(endpoint.ComponentInventory.Any(), "Endpoint component inventory should not be empty.");
    }

    [Test]
    public void SaveShipmentInfo_ShouldSaveMultipleShipments()
    {
        var dataProvider = new DataProvider_FAKE_Version3();
        var shipments = new List<Shipment>
        {
            new Shipment
            {
                Products = new List<Product> { new Product { ProductName = "wood", Quantity = 10 } },
                Sender = new Supplier { Name = "Supplier A" },
                Receiver = new Supplier { Name = "Supplier B" }
            },
            new Shipment
            {
                Products = new List<Product> { new Product { ProductName = "nails", Quantity = 50 } },
                Sender = new Supplier { Name = "Supplier C" },
                Receiver = new Supplier { Name = "Supplier D" }
            }
        };

        Assert.DoesNotThrow(() => dataProvider.SaveShipmentInfo(shipments), "Saving multiple shipments should not throw an exception.");
    }

    [Test]
    public void GetBoxValues_ShouldHaveValidSupplierPosition()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        Assert.IsTrue(suppliers.All(s => s.Position.X >= 0 && s.Position.Y >= 0),
            "Supplier positions should have valid non-negative X and Y coordinates.");
    }

    [Test]
    public void ProductInventory_ShouldContainCorrectUnitsForProducts()
    {
        var dataProvider = new DataProvider_FAKE();
        var suppliers = dataProvider.GetBoxValues();

        var supplier = suppliers.First();
        var product = supplier.supplier.ProductInventory.First(p => p.ProductName == "Saw Blade 2-pack");

        Assert.AreEqual("kg", product.Units, "Expected product 'Saw Blade 2-pack' to have unit 'kg'.");
    }

    [Test]
    public void Endpoint_ShouldContainCorrectProductionList()
    {
        var endpoint = RealEnpointForTest.makeAnEnpointForTest();

        Assert.IsTrue(endpoint.ProductionList.Any(), "Endpoint production list should not be empty.");
        Assert.AreEqual("box", endpoint.ProductionList.First().ResultingProduct.ProductName, "Expected the resulting product of the first product line to be 'box'.");
    }
}

