using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.Strategy.Animals
{
    public class AnimalsManagement
    {
        private List<IAnimal> _animals;

        public AnimalsManagement()
        {
            this._animals = new List<IAnimal>();
        }

        public List<IAnimal> GetAll()
        {
            PopulateAnimals();
            return this._animals;
        }

        private void PopulateAnimals()
        {
            var turtle = new Turtle();
            var elephant = new Elephant();
            var tiger = new Tiger();

            this._animals.Add(turtle);
            this._animals.Add(elephant);
            this._animals.Add(tiger);
        }
    }
}