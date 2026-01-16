using Google.Apis.Services;
using Google.Apis.Translate.v2;
using Google.Apis.Translate.v2.Data;
using TranslationsResource = Google.Apis.Translate.v2.Data.TranslationsResource;

using System;
using System.Collections.Generic;
using System.Linq;

using NLog;

using translate_spa.Models;
using translate_spa.Utilities;

namespace translate_spa.Actions
{
    public class GoogleTranslate
    {
        private readonly Translation _translation;
        private readonly ILogger _log;
        private readonly TranslateService _translateService;

        public GoogleTranslate(Translation translation, ILogger log)
        {
            _translation = translation;
            _log = log;
            _translateService = new TranslateService(new BaseClientService.Initializer
            {
                ApplicationName = "esignatur translations portal",
                    ApiKey = "AIzaSyDAWis6FJJerxydhqV-iPSjChj3cY4E1FQ",
            });
        }

        public Translation Execute()
        {
            //var languages = Enum.GetValues(typeof(Language));
            var languages = Enum.GetValues(typeof(Language))
                .Cast<Language>()
                .Where(x => x != Language.Da);

            foreach (var lang in languages)
            {
                var value = _translation.GetByLanguage(lang);
                if (!string.IsNullOrEmpty(value))
                {
                    continue;
                }
                _translation.SetValueByLanguage(lang, GetTranslation(lang));
            }
            return _translation;
        }

        string GetTranslation(Language distLang)
        {
            //var response = _translateService.Translations.List(_translation.Da, distLang).Execute();
            var response = _translateService.Translations.Translate(new TranslateTextRequest()
            {
                Format = _translation.Da.ContainsXHTML() ? "html" : "text",
                    Source = Language.Da.ToString(),
                    Target = distLang.ToString(),
                    Q = new [] { _translation.Da }
            }).Execute();

            var translation = response.Translations.First();

            _log.Debug($"translation from lang '{Language.Da.ToString()}'  to '{distLang.ToString()}'\n\tSource: {_translation.Da}\n\t\n\tResult: {translation.TranslatedText}");

            return translation.TranslatedText;
        }
    }
}