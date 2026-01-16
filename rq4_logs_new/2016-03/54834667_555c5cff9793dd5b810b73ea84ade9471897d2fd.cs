using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Newtonsoft;
using System.IO;
using Model;


namespace Model_View
{

    public partial class Form1 : Form
    {
        private DataTable _datatable;

        public Form1()
        {
            InitializeComponent();
            CreateDatatable();
            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                column.SortMode = DataGridViewColumnSortMode.NotSortable;
            }
        }

        public class ListFigures
        {
            public static List<IFigure> list = new List<IFigure>();
        }

        private void CreateDatatable()
        {

            _datatable = new DataTable();
            var column0 = new DataColumn("Фигура")
            {
                Caption = "Figure",
                DataType = typeof(string),
                ReadOnly = true
            };
            var column2 = new DataColumn("Обьём")
            {
                Caption = "Volume",
                DataType = typeof(double),
                ReadOnly = true
            };

            _datatable.Columns.Add(column0);
            _datatable.Columns.Add(column2);
            dataGridView1.DataSource = _datatable;

        }


        private void menuStrip1_ItemClicked(object sender, ToolStripItemClickedEventArgs e)
        {

        }

        private void CloseFile_Click(object sender, EventArgs e)
        {
            DialogResult result = MessageBox.Show("Закрыть программу?", "Завершение", MessageBoxButtons.YesNo);
            if (result == DialogResult.Yes) Application.Exit();
        }

        private void SaveFile_Click(object sender, EventArgs e)
        {
            var serializer = new Newtonsoft.Json.JsonSerializer();
            var saveFileDialog = new SaveFileDialog();
            saveFileDialog.Filter = "MyFigure | *.figure";
            if (saveFileDialog.ShowDialog() == DialogResult.Cancel)
                return;
            var fileName = saveFileDialog.FileName;

            using (StreamWriter streamWriter = new StreamWriter(fileName))
            {
                using (Newtonsoft.Json.JsonWriter jWriter = new Newtonsoft.Json.JsonTextWriter(streamWriter))
                {
                    serializer.Serialize(jWriter, ListFigures.list);
                }

                MessageBox.Show("Сохранено");

            }
        }


        private void TableFigure_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {


        }
    

        private void OpenFile_Click(object sender, EventArgs e)
        {
            var serializer = new Newtonsoft.Json.JsonSerializer();
            var openFileDialog = new OpenFileDialog();
            if (openFileDialog.ShowDialog() == DialogResult.Cancel)
                return;
            var fileName = openFileDialog.FileName;

            //using (StreamWriter streamWriter = new StreamWriter(fileName))
            //{
            //    using (Newtonsoft.Json.JsonWriter jWriter = new Newtonsoft.Json.JsonTextWriter(streamWriter))
            //    {
            //        string outputString = Newtonsoft.Json.JsonConvert.DeserializeObject<string>(JReader);
            //        serializer.Deserialize(JReader, ListFigures.list);
            //    }
            //    MessageBox.Show("Файл загружен");
            //}

        }

        private void newToolStripMenuItem_Click(object sender, EventArgs e)
        {
            DialogResult result = MessageBox.Show("Все не сохраннёные данные могут быть потеряны. Создать новый файл?", "New", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
            try
            {
                if (result == DialogResult.Yes)
                {
                    _datatable.Rows.Clear();
                    ListFigures.list.Clear(); 
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void NewFile_Click(object sender, EventArgs e)
        {
            DialogResult result = MessageBox.Show("Все не сохраннёные данные могут быть потеряны. Создать новый файл?", "New", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
            try
            {
                if (result == DialogResult.Yes)
                {
                    _datatable.Rows.Clear();
                    ListFigures.list.Clear(); 
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void buttonAdd_Click(object sender, EventArgs e)
        {

            New_Figures f = new New_Figures();
            IFigure figure = null;
            f.ShowDialog();
            if (f.DialogResult == DialogResult.OK)
            {
                figure = f.Figure;
                ListFigures.list.Add(figure);
                var row = _datatable.NewRow();
                row[0] = figure.Name;
                row[1] = figure.CalculateVolume();
                _datatable.Rows.Add(row);
                dataGridView1.Update();
            }
        }



        private int selectedRow;
        private void buttonRemoveData_Click(object sender, EventArgs e)
        {

            DialogResult result = MessageBox.Show("Удалить обьект?", "Удалить", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
            if (result == DialogResult.Yes)
            {
                foreach (DataGridViewCell cell in dataGridView1.SelectedCells)
                {
                    dataGridView1.Rows.RemoveAt(cell.RowIndex);
                }
            }

        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }


        public IFigure Figure { get; set; }
        private int _figure;
        private void buttonRandomData_Click_2(object sender, EventArgs e)
        {
            Random rnd = new Random(); 

            int style;
            for (int i = 0; i < 10; i++)
            {
                style = rnd.Next(0, 3);
                Model.IFigure figureModel = null;
                switch (_figure)
                {
                    case 0:
                        figureModel = new ball(Convert.ToInt32(rnd.Next(0, 10)));
                        break;
                    case 1:
                        figureModel = new Parallelepiped(rnd.Next(0, 100), rnd.Next(0, 100), rnd.Next(1, 15));
                        break;
                    case 2:
                        figureModel = new Piramide(rnd.Next(0, 100), rnd.Next(0, 100));
                        break;

                }
                Figure = figureModel;
                DialogResult = DialogResult.OK;
                _figure = (rnd.Next(0, 3));
                IFigure figure = null;
                figure = Figure;
                ListFigures.list.Add(figure);
                var row = _datatable.NewRow();
                row[0] = figure.Name;
                row[1] = figure.CalculateVolume();
                _datatable.Rows.Add(row);
            }
           
        }

        private void Result_Enter(object sender, EventArgs e)
        {

        }

        private void Result_Enter_1(object sender, EventArgs e)
        {

        }

        private void backgroundWorker1_DoWork(object sender, DoWorkEventArgs e)
        {

        }
    }

}