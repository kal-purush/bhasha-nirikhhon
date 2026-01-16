using FoodImportLabelGenerator.Data;
using Microsoft.EntityFrameworkCore;

namespace FoodImportLabelGenerator.Repository;

public class LabelRepository : ILabelRepository
{
    private readonly IConfiguration _configuration;
    public LabelRepository(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    
    public IEnumerable<Label> GetAll()
    {
        using var dbContext = new FoodImportLabelGeneratorContext(_configuration);
        return dbContext.Labels.ToList();
    }

    public IEnumerable<Label> GetByName(string legalName)
    {
        using var dbContext = new FoodImportLabelGeneratorContext(_configuration);
        return dbContext.Labels.Where(s => s.LegalName.ToLower().Contains(legalName.ToLower()));
    }

    public Label? GetById(int id)
    {
        using var dbContext = new FoodImportLabelGeneratorContext(_configuration);
        return dbContext.Labels.FirstOrDefault(s=>s.Id == id);
    }

    public void Add(Label label)
    {
        using var dbContext = new FoodImportLabelGeneratorContext(_configuration);
        dbContext.Add(label);
        dbContext.SaveChanges();

    }

    public void Update(Label label)
    {
        using var dbContext = new FoodImportLabelGeneratorContext(_configuration);
        dbContext.Update(label);
        dbContext.SaveChanges();
    }

    public void Delete(Label label)
    {
        using var dbContext = new FoodImportLabelGeneratorContext(_configuration);
        dbContext.Remove(label);
        dbContext.SaveChanges();
    }
}