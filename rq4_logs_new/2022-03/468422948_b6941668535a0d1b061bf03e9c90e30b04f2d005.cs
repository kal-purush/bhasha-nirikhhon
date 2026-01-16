using Autocomplete.Business.Interface;
using Autocomplete.Business.Models;
using Autocomplete.Data.Contexto;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.IO;
using System;
using System.Globalization;

namespace Autocomplete.Business.Repository
{
    public class CityRepository : ICityRepository
    {
        public async Task<IEnumerable<City>> GetAll()
        {
            var files = GetFile();

            var list = await Task.Factory.StartNew(() => files.Select(x => new City(x))
                    .Where(x =>
                            x.IsOk() 
                        )
                    .OrderBy(x => x.original)

                .ToList());

            return list;
        }


        public async Task<IEnumerable<City>> GetCityByFilterAsync(FiltroViewModel filtro)
        {
            var files = GetFile();

            var list = await Task.Factory.StartNew(() => files.Select(x => new City(x))
                    .Where(x =>
                            x.IsOk() && (string.IsNullOrEmpty(filtro?.Filtro?.Trim()) || x.original.ToLower().Contains(filtro.Filtro?.ToLower()))
                        )
                    .OrderBy(x => x.original)

                .Take(50).ToList());

            return list;
        }

        private IList<string> GetFile()
        {


            string path = Path.Combine(Environment.CurrentDirectory.Replace(@"Autocompletetest\bin\Debug\net5.0", "AutoComplete"), @"world-cities_csv.csv");
            if (!File.Exists(path))
                return new List<string>();

            var files = File.ReadAllLines(path);

            if (files.Any(x => !string.IsNullOrEmpty(x) && x.Split(",").Count() == 4))
                return files.Distinct().Where(x => !string.IsNullOrEmpty(x) && x.Split(",").Count() == 4).ToList();
            else
                return files.Distinct().Where(x => !string.IsNullOrEmpty(x) && x.Split(";").Count() == 4).ToList();
        }
    }
}