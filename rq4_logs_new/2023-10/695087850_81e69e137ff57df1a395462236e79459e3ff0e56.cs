using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc; //needed because this class derives from ControllerBase

namespace FoodImportStickerGenerator.Controllers;

[ApiController]
[Route("[controller]")]
public class LabelController : ControllerBase
{

    private static readonly List<Label> Labels = new()
    {
        new Label()
        {
            Id = 1,
            Date = DateOnly.FromDateTime(DateTime.Now),
            LegalName = "Chocolate",
            Nutritions = "calories",
            Producer = "Mayu Kft.",
            CountryOfOrigin = "Hungary",
            NetWeight = 500,
            Storage = "20 °C",
            BBD = new DateTime(2023, 12,12),
            Organic = false
            
        },
        new Label()
        {
            Id = 2,
            Date = DateOnly.FromDateTime(DateTime.Now),
            LegalName = "Candy",
            Nutritions = "calories",
            Producer = "Mayu Kft.",
            CountryOfOrigin = "Hungary",
            NetWeight = 300,
            Storage = "25 °C",
            BBD = new DateTime(2023, 10,30),
            Organic = true
            
        }
    };
    
    private readonly ILogger<LabelController> _logger;

    public LabelController(ILogger<LabelController> logger)
    {
        _logger = logger;
    }

    [HttpGet("GetAll")]
    public ActionResult<IEnumerable<Label>> GetAll()
    {
        return Ok(Labels);
    }

    [HttpGet("GetByName")]
    public ActionResult<IEnumerable<Label>> GetByName(string name)
    {
        var stickersByName = Labels.Where(s => s.LegalName.ToLower().Contains(name.ToLower()));
        if (!stickersByName.Any())
        {
            return NotFound($"There is no label with name {name}.");
        }
        return Ok(stickersByName);
    }
    
    [HttpGet("GetById/{id}")]
    public ActionResult<Label> GetById(int id)
    {
        Label existingLabel = Labels.FirstOrDefault(s=>s.Id == id);
        
        if (existingLabel == null)
        {
            return NotFound($"There is no label with id {id}.");
        }

        return Ok(existingLabel);
    }
    
    [HttpPost("Add")]
    public ActionResult<IEnumerable<Label>> Add(string? productName, [Required]string legalName,
        string? ingredientsList, string? allergens, [Required]string nutritions, string? producer,
        [Required]string distributor, string? countryOfOrigin, int netWeight, int netVolume,
        [Required]string storage, DateTime? ubd, DateTime? bbd, DateTime? bbe, [Required]bool organic)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest("Missing or invalid input data");
        }
        
        Label newLabel = new Label()
        {
            Id = Labels.Count()+1,
            Date = DateOnly.FromDateTime(DateTime.Now),
            ProductName = productName,
            LegalName = legalName,
            IngredientsList = ingredientsList,
            Allergens = allergens,
            Nutritions = nutritions,
            Producer = producer,
            Distributor = distributor,
            CountryOfOrigin = countryOfOrigin,
            NetWeight = netWeight,
            NetVolume = netVolume,
            Storage = storage,
            UBD = ubd,
            BBD = bbd,
            BBE = bbe,
            Organic = organic
        };
        Labels.Add(newLabel);
        
        return Ok(Labels);
    }

    [HttpPut("Update/{id}")]
    public ActionResult<Label> Update(int id, string? productName, string? legalName, string? ingredientsList,
        string? allergens, string? nutritions, string? producer, string? distributor, string? countryOfOrigin,
        int netWeight, int netVolume, string? storage, DateTime ubd, DateTime bbd, DateTime bbe, bool organic)
    {
        Label existingLabel = Labels.FirstOrDefault(s => s.Id == id);

        if (existingLabel == null)
        {
            return NotFound($"It is not possible to update label. There is no label with id {id}.");
        }

        existingLabel.ProductName = string.IsNullOrEmpty(productName) ? existingLabel.ProductName : productName;
        existingLabel.LegalName = string.IsNullOrEmpty(legalName) ? existingLabel.LegalName : legalName;
        existingLabel.IngredientsList = string.IsNullOrEmpty(ingredientsList) ? existingLabel.IngredientsList : ingredientsList;
        existingLabel.Allergens = string.IsNullOrEmpty(allergens) ? existingLabel.Allergens : allergens;
        existingLabel.Nutritions = string.IsNullOrEmpty(nutritions) ? existingLabel.Nutritions : nutritions;
        existingLabel.Producer = string.IsNullOrEmpty(producer) ? existingLabel.Producer : producer;
        existingLabel.Distributor = string.IsNullOrEmpty(distributor) ? existingLabel.Distributor : distributor;
        existingLabel.CountryOfOrigin = string.IsNullOrEmpty(countryOfOrigin) ? existingLabel.CountryOfOrigin : countryOfOrigin;
        existingLabel.NetWeight = netWeight == 0 ? existingLabel.NetWeight : netWeight;
        existingLabel.NetVolume = netVolume == 0 ? existingLabel.NetVolume :netVolume;
        existingLabel.Storage = string.IsNullOrEmpty(storage) ? existingLabel.Storage : storage;
        existingLabel.UBD = ubd == new DateTime() ? existingLabel.UBD : ubd;
        existingLabel.BBD = bbd == new DateTime() ? existingLabel.BBD : bbd;
        existingLabel.BBE = bbe == new DateTime() ? existingLabel.BBE : bbe;
        existingLabel.Organic = organic == new Boolean() ? existingLabel.Organic : organic;

        return Ok(existingLabel);
    }
    
    [HttpDelete("DeleteById/{id}")]
    public ActionResult<Label> DeleteById(int id)
    {
        Label existingLabel = Labels.FirstOrDefault(s=>s.Id == id);
        
        if (existingLabel == null)
        {
            return NotFound($"It is not possible to delete label. There is no label with id {id}.");
        }

        Labels.Remove(existingLabel);
        return Ok(Labels);
    }
    
}