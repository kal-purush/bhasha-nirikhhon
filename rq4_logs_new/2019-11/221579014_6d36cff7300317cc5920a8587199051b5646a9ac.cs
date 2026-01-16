using ChajdPizzaWebApp.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChajdPizzaWebApp.Repositories.Interfaces
{
    public interface IPizzaTypesRepo
    {
        public Task<IEnumerable<SecretFormula>> GetSecretFormulas();

        public Task<SecretFormula> GetSecretFormula(int id);

        public Task<decimal> GetSecretFormulaPrice(int id);

        public Task<IEnumerable<Size>> GetPizzaSizes();

        public Task<Size> GetPizzaSize(int id);

        public Task<string> GetPizzaSizeName(int id);

        public Task<decimal> GetPizzaSizePrice(int id);

        public Task<IEnumerable<SpecialtyPizza>> GetSpecialtyPizzas();

        public Task<SpecialtyPizza> GetSpecialtyPizza(int id);

        public Task<double> GetSpecialtyPizzaPrice(int id);

        public Task<string> GetSpecialtyPizzaName(int id);

        public Task<string> GetSpecialtyPizzaDescription(int id);

        public Task<IEnumerable<Toppings>> GetToppings();

        public Task<Toppings> GetTopping(int id);

        public Task<string> GetToppingName(int id);
    } 
}