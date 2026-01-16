using assigMVCPeople.Models.Repos;
using assigMVCPeople.Models.ViewModels;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace assigMVCPeople.Models.Services
{
    public class LanguageService : ILanguageService
    {
        ILanguageRepo _languageRepo;
        private readonly IPeopleRepo _peopleRepo;
        public LanguageService(ILanguageRepo languageRepo, IPeopleRepo peopleRepo)
        {
            _languageRepo = languageRepo;
            _peopleRepo = peopleRepo;
        }

        public Language Create(CreateLanguageViewModel createLanguage)
        {
            if (string.IsNullOrWhiteSpace(createLanguage.LanguageName))
            {
                throw new ArgumentNullException("LanguageName need an input");
            }

            var person = _peopleRepo.Read(createLanguage.Id);
            if(person == null)
            {
                throw new ArgumentException("Person need to have a value");
            }

            Language language = new Language()
            {
                LanguageName = createLanguage.LanguageName,
               // Person = person
            };
            return _languageRepo.Create(language);
        }
       
         public List<Language> GetAll()
        {
            return _languageRepo.Read();
        }
        public Language FindById(int id)
        {
            Language language = _languageRepo.Read(id);
            return language;
        }

        public bool Edit(int id, CreateLanguageViewModel editLanguage)
        {
            Language language = FindById(id);
            if(language != null)
            {
                language.LanguageName = editLanguage.LanguageName;
            }
            return _languageRepo.Update(language);
        }

        public bool Remove(int id)
        {
            Language language = _languageRepo.Read(id);
            bool success = _languageRepo.Delete(language);
            return success;
        }

        public List<Language> Search(string search)
        {
            List<Language> searchLanguage = _languageRepo.Read();
            foreach (Language language in _languageRepo.Read())
            {
                if (language.LanguageName.Contains(search, StringComparison.OrdinalIgnoreCase))
                {
                    searchLanguage = searchLanguage.Where(p => p.LanguageName.Contains(search.ToUpper())).ToList();
                    searchLanguage.Add(language);
                }
            }
            if (searchLanguage.Count == 0)
            {
                throw new ArgumentException("Could not find, try another");
            }
            return searchLanguage;
        }
    }
}