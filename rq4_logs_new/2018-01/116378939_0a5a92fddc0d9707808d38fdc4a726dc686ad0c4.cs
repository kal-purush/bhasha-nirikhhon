using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmamForms
{
    class Article
    {
        //fields
        private List<Image> images;
        private int id;
        private string name;
        private string description;
        
        public Article() //constructor
        {
            images = new List<Image>();
        }

        //properties
        public List<Image> Images { get => images; set => images = value; }
        public int Id { get => id; set => id = value; }
        public string Name { get => name; private set => name = value; }
        public string Description { get => description; private set => description = value; }

        public void SetArticleName(string name)
        {
            this.name = name;
        }

        public void SetArticleDesc(string desc)
        {
            this.description = desc;
        }

        public void AddImageToArticle(Image image)
        {
            images.Add(image);
        }

        public override string ToString()
        {
            return name + " - " + description;
        }
    }
}