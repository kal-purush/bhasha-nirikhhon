using System;
using System.Collections.Generic;
using System.Text;

namespace FifoAnimalShelter
{
    public abstract class Animal
    {
        // Give SerialNumber a negative value to distinguish Animal
        // objects which have not been added to an animal shelter from
        // the first animal added to an animal shelter. Using long
        // allows the animal shelter to enqueue a _lot_ of animals
        // before worrying about the numbers wrapping
        public long SerialNumber { get; set; } = long.MinValue;
        public abstract string Phrase { get; }
    }
}