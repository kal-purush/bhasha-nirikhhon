using FoodImportLabelGenerator.Models;

namespace FoodImportLabelGenerator.Repository;

public interface ITranslationRepository
{
    IEnumerable<Translation> GetAll();
    IEnumerable<Translation> GetByEnglishWord(string englishWord); //if the value was not found, the method will return null
    IEnumerable<Translation> GetByHungarianWord(string hungarianWord);
    Translation? GetById(int id);
    void Add(Translation translation);
    void Update(Translation updatedTranslation);
    void Delete(Translation translation);
}