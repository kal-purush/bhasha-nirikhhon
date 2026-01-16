using System;
using System.IO;
using System.Windows.Forms;

namespace ScottSaveEditor
{
    public partial class Form1 : Form
    {
        public string output_win;

        public Form1()
        {
            InitializeComponent();
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void openToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var openFileDialog1 = new OpenFileDialog();
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
                output_win = openFileDialog1.FileName;
            else
                return;

            BinaryReader bread = new BinaryReader(File.Open(output_win, FileMode.Open));
            bread.BaseStream.Position = 0x2A8;

            string[] characters = { "Scott", "Kim", "Stills", "Ramona", "NegaScott", "Knives", "Wallace" };

            dataGridView1.Rows.Clear();

            for (int i = 0; i < 7; i++)
            {
                dataGridView1.Rows.Add();
                dataGridView1[0, i].Value = characters[i];

                dataGridView1[1, i].Value = bread.ReadSingle().ToString();
                dataGridView1[2, i].Value = bread.ReadSingle().ToString();
                dataGridView1[3, i].Value = bread.ReadSingle().ToString();
                dataGridView1[4, i].Value = bread.ReadSingle().ToString();

                bread.BaseStream.Position += 24;

                dataGridView1[5, i].Value = bread.ReadSingle().ToString();
                dataGridView1[6, i].Value = bread.ReadSingle().ToString();

                bread.BaseStream.Position += 4;

                dataGridView1[7, i].Value = bread.ReadSingle().ToString();
                dataGridView1[8, i].Value = bread.ReadInt32().ToString();

                bread.BaseStream.Position += 12;

                dataGridView1[9, i].Value = bread.ReadSingle().ToString();
                dataGridView1[10, i].Value = bread.ReadSingle().ToString();
                dataGridView1[11, i].Value = (bread.ReadInt32() + 1).ToString();

                bread.BaseStream.Position += 28;
            }

            bread.BaseStream.Position = 0x80;
            checkBox1.Checked = bread.ReadByte() != 0;

            bread.Close();
            saveToolStripMenuItem.Enabled = true;
        }

        private void saveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            BinaryWriter bread = new BinaryWriter(File.Open(output_win, FileMode.Open));
            bread.BaseStream.Position = 0x2A8;

            for (int i = 0; i < 7; i++)
            {
                bread.Write(Single.Parse((string)dataGridView1[1, i].Value));
                bread.Write(Single.Parse((string)dataGridView1[2, i].Value));
                bread.Write(Single.Parse((string)dataGridView1[3, i].Value));
                bread.Write(Single.Parse((string)dataGridView1[4, i].Value));

                bread.BaseStream.Position += 24;

                bread.Write(Single.Parse((string)dataGridView1[5, i].Value));
                bread.Write(Single.Parse((string)dataGridView1[6, i].Value));

                bread.BaseStream.Position += 4;

                bread.Write(Single.Parse((string)dataGridView1[7, i].Value));
                bread.Write(Int32.Parse((string)dataGridView1[8, i].Value));

                bread.BaseStream.Position += 12;

                bread.Write(Single.Parse((string)dataGridView1[9, i].Value));
                bread.Write(Single.Parse((string)dataGridView1[10, i].Value));
                bread.Write(Int32.Parse((string)dataGridView1[11, i].Value) - 1);

                bread.BaseStream.Position += 28;
            }

            bread.BaseStream.Position = 0x80;
            bread.Write((byte)(checkBox1.Checked?1:0));

            bread.Close();
            MessageBox.Show("File saved!");
        }

        private void aboutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            MessageBox.Show(Application.ProductName + "\n\nVersion : 1.0.0.0\n\nCreated by : fjay69\n\nBased on\n\nScott Pilgrim Vs The World - Save Editor\n\nby JizzaBeez");
        }
    }
}