using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KaibaReduxAPI.Models
{
    public class Item
        // repersents a single menu item
    {
        // class variables
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }

        // this number represents the position within the section for this item
        // ie. Is this item first, fifth, or seventh
        public float Position { get; set; }

        // this holds the path to the picture for this item
        public string PicturePath { get; set; }

        // the list of prices for this item
        public List<Priceline> PriceLineList { get; set; }

        public Item(int id, string name, string description, float position, string picturePath)
            // Standard constructor- takes and assigns the class variables
        {
            Id = id;
            Name = name;
            Description = description;
            Position = position;
            PicturePath = picturePath;
            PriceLineList = new List<Priceline>();
        }

        public void addPriceline(Priceline price)
        {
            PriceLineList.Add(price);
        }

        public List<Priceline> getPricelineList()
        {
            return PriceLineList;
        }
    }
}