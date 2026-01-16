using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Model
{
    class CategoryImg
    {
        private int id;

        public int Id
        {
            get { return id; }
            set { id = value; }
        }
        private String imgPath;

        public String ImgPath
        {
            get { return imgPath; }
            set { imgPath = value; }
        }
        private Category category;

        public Category Category
        {
            get { return category; }
            set { category = value; }
        }
    }
}