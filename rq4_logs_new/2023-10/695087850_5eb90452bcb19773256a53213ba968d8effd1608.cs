using System.Collections;
using FoodImportLabelGenerator;
using FoodImportLabelGenerator.Controllers;
using Microsoft.AspNetCore.Mvc;

namespace FoodImportLabelGeneratorTests;

public class LabelControllerUnitTests
{
    private LabelController _labelController;
    
    [SetUp]
    public void Setup()
    {
        // Initializing the LabelController with a Mock ILogger:
        _labelController = new LabelController(new Microsoft.Extensions.Logging.Abstractions.NullLogger<LabelController>());
    }

    [Test]
    public void  GetAll_ReturnsOkResult()
    {
        // Act
        ActionResult<IEnumerable<Label>> result = _labelController.GetAll();

        // Assert
        Assert.IsInstanceOf<OkObjectResult>(result.Result);
    }
    
    [Test]
    public void GetByName_ReturnsOkResult()
    {
        // Arrange
        string name = "Chocolate";

        // Act
        ActionResult<IEnumerable<Label>> result = _labelController.GetByName(name);

        // Assert
        Assert.IsInstanceOf<OkObjectResult>(result.Result);
    }
    
    [Test]
    public void GetByName_ReturnsNotFoundResult()
    {
        // Arrange
        string name = "NonexistentLabelName";

        // Act
        ActionResult<IEnumerable<Label>> result = _labelController.GetByName(name);

        // Assert
        Assert.IsInstanceOf<NotFoundObjectResult>(result.Result);
    }
    
    [Test]
    public void GetById_ReturnsOkResult()
    {
        // Arrange
        int id = 1;

        // Act
        ActionResult<Label> result = _labelController.GetById(id);

        // Assert
        Assert.IsInstanceOf<OkObjectResult>(result.Result);
    }

    [Test]
    public void GetById_ReturnsNotFoundResult()
    {
        // Arrange
        int id = 999999999; // Non existing ID

        // Act
        ActionResult<Label> result = _labelController.GetById(id);

        // Assert
        Assert.IsInstanceOf<NotFoundObjectResult>(result.Result);
    }

    [Test]
    public void Add_ReturnsOkResult()
    {
        // Arrange: Valid details for Add
        string legalName = "NewLabel";

        // Act
        ActionResult<IEnumerable<Label>> result = _labelController.Add(null, legalName, null, null, "NewNutritions", null, "NewDistributor", "NewCountry", 500, 0, "NewStorage", null, null, null, true);

        // Assert
        Assert.IsInstanceOf<OkObjectResult>(result.Result);
    }

    [Test]
    public void Add_ReturnsBadRequestResultForInvalidInput()
    {
        // Arrange: Non valid details for Add
        string legalName = null;

        // Act
        ActionResult<IEnumerable<Label>> result = _labelController.Add(null, legalName, null, null, "NewNutritions", null, "NewDistributor", "NewCountry", 500, 0, "NewStorage", null, null, null, true);

        // Assert
        Assert.IsInstanceOf<BadRequestObjectResult>(result.Result);
    }

    [Test]
    public void Update_ReturnsOkResult()
    {
        // Arrange: Existing label ID and valid modifications
        int id = 1;
        string newProductName = "NewProductName";
        string newLegalName = "UpdatedLabelName";

        // Act
        ActionResult<Label> result = _labelController.Update(id, newProductName, newLegalName, null, null, null, null, null, null, 0, 0, null, DateTime.Now, DateTime.Now, DateTime.Now, false);

        // Assert
        Assert.IsInstanceOf<OkObjectResult>(result.Result);
    }

    [Test]
    public void Update_ReturnsNotFoundResult()
    {
        // Arrange: Non existing label ID and modifications
        int id = 999;
        string newProductName = "NewProductName";
        string newLegalName = "UpdatedLabelName";

        // Act
        ActionResult<Label> result = _labelController.Update(id, newProductName, newLegalName, null, null, null, null, null, null, 0, 0, null, DateTime.Now, DateTime.Now, DateTime.Now, false);

        // Assert
        Assert.IsInstanceOf<NotFoundObjectResult>(result.Result);
    }
/*
    [Test]
    public void DeleteById_ReturnsOkResult()
    {
        // Arrange: Existing label ID
        int id = 1;

        // Act
        ActionResult<Label> result = _labelController.DeleteById(id);

        // Assert
        Assert.IsInstanceOf<OkObjectResult>(result.Result);
    }

    [Test]
    public void DeleteById_ReturnsNotFoundResult()
    {
        // Arrange: Non existing label ID
        int id = 999999999;

        // Act
        ActionResult<Label> result = _labelController.DeleteById(id);

        // Assert
        Assert.IsInstanceOf<NotFoundObjectResult>(result.Result);
    }
*/
}