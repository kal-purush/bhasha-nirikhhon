using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using Rems.Application.Common.Extensions;

namespace WindowsClient.Models
{
    public interface IExcelData
    {
        event Action<string, object> StateChanged;

        object Tag { get; }

        string Name { get; set; }

        DataTable Source { get; }

        PropertyCollection State { get; }

        List<RichText> valid { get; set; }
        List<RichText> invalid { get; set; }

        List<MenuItem> Items { get; }

        void SetMenu(params MenuItem[] items);
    }

    public class ExcelTable : IExcelData
    {
        readonly DataTable table;

        public object Tag => table;

        public event Action<string, object> StateChanged;

        public string Name
        {
            get => table.TableName;
            set => table.TableName = value;
        }

        public DataTable Source => table;

        public PropertyCollection State => table.ExtendedProperties;

        public List<RichText> valid { get; set; } = new List<RichText>
        {
            new RichText
            { Text = "This table is valid. Check the other tables prior to import.", Color = Color.Black }
        };

        public List<RichText> invalid { get; set; } = new List<RichText>
        {
            new RichText
            { Text = "This table contains columns that REMS does not recognise. " +
            "Please fix the columns before importing", Color = Color.Black }
        };

        public List<MenuItem> Items { get; } = new List<MenuItem>();

        public ExcelTable(DataTable table)
        {
            this.table = table;

            State["Ignore"] = false;
            State["Valid"] = true;            
        }

        public void SetMenu(params MenuItem[] items)
        {
            // Not used
        }
    }

    public class ExcelColumn : IExcelData
    {
        readonly DataColumn column;

        public object Tag => column;

        public event Action<string, object> StateChanged;

        public string Name
        {
            get => column.ColumnName;
            set => column.ColumnName = value;
        }

        public DataTable Source => column.Table;

        public PropertyCollection State => column.ExtendedProperties;

        public List<RichText> valid { get; set; } = new List<RichText>
        {
            new RichText
            { Text = "This column is valid and can be imported", Color = Color.Black }
        }; 
        
        public List<RichText> invalid { get; set; } = new List<RichText>
        {
            new RichText
            { Text = "The type of column could not be determined. ", Color = Color.Black },
            new RichText
            { Text = "Right click to view options. \n\n", Color = Color.Black },
            new RichText
            { Text = "Ignore\n", Color = Color.Blue },
            new RichText
            { Text = "    - The column is not imported\n\n", Color = Color.Black },
            new RichText
            { Text = "Add trait\n", Color = Color.Blue },
            new RichText
            { Text = "    - Add a trait named for the column\n", Color = Color.Black },
            new RichText
            { Text = "    - Only valid traits are imported\n\n", Color = Color.Black },
            new RichText
            { Text = "Set property\n", Color = Color.Blue },
            new RichText
            { Text = "    - Match the column to a REMS property\n", Color = Color.Black }
        };

        public List<MenuItem> Items { get; } = new List<MenuItem>();

        public ExcelColumn(DataColumn column)
        {
            this.column = column;

            State["Ignore"] = false;                       
        }

        public void SetMenu(params MenuItem[] items)
        {
            if (State["Info"] != null)
            {
                items[1].Enabled = false;
                return;
            }

            items[2].MenuItems.Clear();

            var props = column.GetUnmappedProperties();

            foreach (var prop in props)
                items[2].MenuItems.Add(prop.Name, SetProperty);
        }

        private void SetProperty(object sender, EventArgs args)
        {
            var item = sender as MenuItem;
            Name = item.Text;
            State["Info"] = column.FindProperty();
            StateChanged?.Invoke("Valid", true);
        }
    }
}