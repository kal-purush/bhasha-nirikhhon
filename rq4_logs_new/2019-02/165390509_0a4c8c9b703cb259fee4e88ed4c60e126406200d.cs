using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsFormsApplication1
{
    public partial class FormUniqueForm : Form
    {
        String FormName;
        public FormUniqueForm(String name)
        {
            InitializeComponent();
            FormName = name;
        }

        /// <summary>
        /// Загрузка дизайна конкретной формы из БД
        /// </summary>
        public static void GetFormDesignFromDb(ref Control c)
        {
            List<String> uniqueDesign = SQLClass.Select("SELECT design FROM " + Tables.Unique + " WHERE FormFrom = '" + c.Name + "'and Type='Form'");
            String[] words = uniqueDesign[0].Split(new string[] { ":", ",", " = ", "=" }, StringSplitOptions.RemoveEmptyEntries);
            
            for (int i = 0; i < words.Length; i++)
            {
                if (words[i] == "Color")
                {
                    foreach (String colorName in Enum.GetNames(typeof(KnownColor)))
                    {
                        String colorFromDB = words[i + 1];
                        if (colorFromDB.Trim().StartsWith("Color"))
                        {
                            colorFromDB = colorFromDB.Trim().Replace("Color [", "").Replace("]", "");
                        }

                        if (colorName == colorFromDB.Trim())
                        {
                            Color knownColor = Color.FromKnownColor((KnownColor)Enum.Parse(typeof(KnownColor), colorName));
                            c.BackColor = knownColor;
                        }
                    }
                }
            }
        }


        private void FormThisDesign_Load(object sender, EventArgs e)
        {     
        }

        private void button1_Click(object sender, EventArgs e)
        {
            fo.ShowColor = true;
            fo.ShowDialog();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            cl.ShowDialog();
        }

        private void button3_Click(object sender, EventArgs e)
        {
            SQLClass.Delete("DELETE FROM designDiffirent WHERE FormFrom = '" + FormName + "' and type = 'Form'");
            SQLClass.Insert("INSERT INTO designDiffirent (type, design, FormFrom, Author, Name)" +
                " VALUES ('Form', " + "'Color: " + Convert.ToString(cl.Color) + "'," + "'" + FormName + "', '', '')");
        }
    }
}