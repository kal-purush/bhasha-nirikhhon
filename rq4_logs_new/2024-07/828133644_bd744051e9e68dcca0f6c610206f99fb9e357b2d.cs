using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace viswaGoldenTreasureGame
{
    public class Area
    {
        #region DATA MEMBER

        private int areaNum;
        private string name;
        private Image image;
        private string description;
        private List<Items> listItems;
        private List<Enemy> listEnemy;

        #endregion

        #region CONSTRUCTOR
        public Area(int areaNum, string name, Image image, string description)
        {
            this.AreaNum = areaNum;
            this.Name = name;
            this.Image = image;
            this.Description = description;
            this.ListItems = new List<Items>();
            this.ListEnemy = new List<Enemy>();
        }
        #endregion

        #region PROPERTIES
        public int AreaNum
        {
            get => areaNum;
            set => areaNum = value;
        }
        public string Name
        {
            get => name;
            set => name = value;
        }
        public Image Image
        {
            get => image;
            set => image = value;
        }
        public string Description
        {
            get => description;
            set => description = value;
        }
        public List<Items> ListItems { get => listItems; set => listItems = value; }
        public List<Enemy> ListEnemy { get => listEnemy; set => listEnemy = value; }
        #endregion

        #region METHOD
        public string DisplayData()
        {
            string data = Name;
            return data;
        }
        public string DisplayDesc()
        {
            string data = Description;
            return data;
        }
        public void DisplayArea(Control container)
        {
            container.BackgroundImage = Image;
            foreach (Items item in ListItems)
            {
                item.DisplayPicture(container);
            }
            foreach (Enemy enemy in ListEnemy)
            {
                enemy.DisplayPicture(container);
            }
        }
        public void AddItem(Items newItem)
        {
            ListItems.Add(newItem);
        }

        public void AddEnemy(Enemy enemy)
        {
            ListEnemy.Add(enemy);
        }

        public void RemoveItem(Items item)
        {
            ListItems.Remove(item);
        }

        public void RemoveEnemy(Enemy enemy)
        {
            ListEnemy.Remove(enemy);
        }

        public void ShowItem(Items item)
        {
            item.Picture.Visible = true;
        }

        public void HideItem(Items item)
        {
            item.Picture.Visible = false;
        }

        public void HideAllItems()
        {
            foreach (Items item in ListItems)
            {
                item.Picture.Visible = false;
            }
        }

        public void HideAllEnemy()
        {
            foreach (Enemy enemy in ListEnemy)
            {
                enemy.Picture.Visible = false;
            }
        }

        public void HideAllProjectiles()
        {
            foreach (Enemy enemy in ListEnemy)
            {
                foreach (Projectile projectile in enemy.ListProjectiles)
                {
                    projectile.Picture.Visible = false;
                }
            }
        }

        public void ShowAllItem()
        {
            foreach (Items item in ListItems)
            {
                item.Picture.Visible = true;
            }
        }

        public void ShowAllEnemy(Control container)
        {
            foreach (Enemy enemy in ListEnemy)
            {
                enemy.DisplayPicture(container);
            }
        }
        #endregion
    }
}