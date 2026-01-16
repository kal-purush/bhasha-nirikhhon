using Aplication.Interfaces.Observer;
using Models.Observer;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Web;

namespace Aplication.Services.Observer
{
    public class IdiomaService
    {
        private readonly List<IIdiomaService> _observers = new List<IIdiomaService>();
        private string _currentLanguage;
        private Dictionary<string, string> _currentTranslations;

        public string CurrentLanguage
        {
            get => _currentLanguage;
            set
            {
                if (_currentLanguage != value)
                {
                    _currentLanguage = value;
                    LoadTranslations(_currentLanguage);
                    NotifyObservers();
                }
            }
        }

        public void Subscribe(IIdiomaService observer)
        {
            _observers.Add(observer);
        }

        public void Unsubscribe(IIdiomaService observer)
        {
            _observers.Remove(observer);
        }

        private void NotifyObservers()
        {
            foreach (var observer in _observers)
            {
                observer.UpdateLanguage(_currentLanguage);
            }
        }

        private void LoadTranslations(string languageCode)
        {
            string filePath = HttpContext.Current.Server.MapPath($"~/Lang/{languageCode}.json");
            if (File.Exists(filePath))
            {
                try
                {
                    string json = File.ReadAllText(filePath);

                    _currentTranslations = JsonConvert.DeserializeObject<Dictionary<string, string>>(json)
                                           ?? new Dictionary<string, string>();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al cargar el archivo JSON de traducción: {ex.Message}");
                    _currentTranslations = new Dictionary<string, string>();
                }
            }
            else
            {
                Console.WriteLine($"Archivo de idioma '{filePath}' no encontrado.");
                _currentTranslations = new Dictionary<string, string>();
            }
        }

        public string GetTranslation(string key)
        {
            if (_currentTranslations == null)
            {
                Console.WriteLine("Advertencia: _currentTranslations es null. Verifique la carga de las traducciones.");
                return key;
            }
            return _currentTranslations.TryGetValue(key, out var translation) ? translation : key;
        }
    }
}