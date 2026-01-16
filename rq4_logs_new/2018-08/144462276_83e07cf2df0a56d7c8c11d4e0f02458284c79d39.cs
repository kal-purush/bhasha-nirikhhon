using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NLog;

using translate_spa.Models;
using translate_spa.Models.Interfaces;
using translate_spa.Repositories;
using translate_spa.Utilities;

namespace translate_spa.Actions
{
    public class AddOnNewBranchTranslation
    {
        readonly MongoRepository<Translation> _mongoRepository;
        readonly ILogger _log;

        public AddOnNewBranchTranslation(MongoRepository<Translation> baseRepository, ILogger log)
        {
            _mongoRepository = baseRepository;
            _log = log;
        }

        public void Execute(List<Translation> translations, string branch)
        {
            PrepareTranslations(translations, branch);
            _mongoRepository.AddMany(translations);
        }

        void SetEnvionment(IEnumerable<Translation> translations)
        {
            var translationsWithoutEnvironment = translations.Where(x => x.Environment == null);
            foreach (var item in translationsWithoutEnvironment)
            {
                new SetEnvironmentFromKey(item).Execute();
            }
        }

        public async Task<Task> ExecuteAsync(List<Translation> translations, string branch)
        {
            PrepareTranslations(translations, branch);
            return _mongoRepository.AddManyAsync(translations);
        }

        void PrepareTranslations(List<Translation> translations, string branch)
        {
            if (string.IsNullOrWhiteSpace(branch))
            {
                throw new ArgumentException($"Cant copy translations to empty branch");
            }

            _log.Debug($"Copying '{translations.Count}' translations to branch: {branch}");

            foreach (var item in translations)
            {
                item.SetNewId();
            }

            SetEnvionment(translations);
        }
    }

}