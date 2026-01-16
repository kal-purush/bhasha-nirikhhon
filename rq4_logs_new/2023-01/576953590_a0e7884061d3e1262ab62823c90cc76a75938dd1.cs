using ClosedXML.Excel;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace StudentManagementSystem.Classes
{
    internal class MakeReport
    {
        private char[] alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".ToCharArray();
        private string filePath;
        private string[,] saveValue;
        private Point startPoint;
        private string templatePath;
        private List<(string value, Point location)> otherValue;

        public List<(string value, Point location)> OrtherValue
        {
            get { return otherValue; }
            set { otherValue = value; }
        }


        public string TemplatePath
        {
            get { return templatePath; }
            set { templatePath = value; }
        }


        public Point StartPoint
        {
            get { return startPoint; }
            set { startPoint = value; }
        }
        public string[,] SaveValue
        {
            get { return saveValue; }
            set { saveValue = value; }
        }
        public string FilePath
        {
            get { return filePath; }
            set { filePath = value; }
        }

        public MakeReport(string _templatePath, Point start, string[,] _saveValue)
        {
            this.templatePath = _templatePath;
            this.startPoint = start;
            this.saveValue = _saveValue;
        }

        public void GetSavePathWithSaveFileDialog()
        {
            using (SaveFileDialog saveFileDialog = new SaveFileDialog())
            {
                saveFileDialog.InitialDirectory = @"C:\";
                saveFileDialog.Title = "Save Excel File";
                //saveFileDialog.CheckFileExists = true;
                //saveFileDialog.CheckPathExists = true;
                saveFileDialog.Filter = "Excel files (*.xlsx)|*.xlsx|Excel files (*.xls)|*.xls";
                saveFileDialog.FilterIndex = 1;
                saveFileDialog.RestoreDirectory = true;
                if (saveFileDialog.ShowDialog() == DialogResult.OK)
                {
                    filePath = saveFileDialog.FileName;
                }
            }
        }

        public bool OverwritetoExcelFile()
        {
            if (string.IsNullOrEmpty(templatePath) || string.IsNullOrEmpty(filePath) || startPoint == null)
            {

                return false;
            }
            try
            {
                using (var wb = new XLWorkbook(templatePath))
                {
                    var ws = wb.Worksheet(1);
                    int rowsCount = saveValue.GetLength(0);
                    int colsCount = saveValue.GetLength(1);
                    for (int i = 0; i < rowsCount; i++)
                    {
                        int rowIdx = i + startPoint.X;
                        int colIdx;

                        for (int j = 0; j < colsCount; j++)
                        {
                            colIdx = j + startPoint.Y;
                            ws.Cell(rowIdx, colIdx).SetValue(saveValue[i, j].Replace(',', '.'));
                            ws.Cell(rowIdx, colIdx).Style.Border.LeftBorder = XLBorderStyleValues.Thin;
                            ws.Cell(rowIdx, colIdx).Style.Border.RightBorder = XLBorderStyleValues.Thin;
                            ws.Cell(rowIdx, colIdx).Style.Border.BottomBorder = XLBorderStyleValues.Thin;
                        }
                    }
                    if (otherValue != null && otherValue.Count > 0)
                    {
                        foreach (var tmp in otherValue)
                        {
                            ws.Cell(tmp.location.X, tmp.location.Y).SetDataType(XLDataType.Text);
                            ws.Cell(tmp.location.X, tmp.location.Y).SetValue(tmp.value);                         
                        }
                    }
                    wb.SaveAs(filePath);
                }

            }
            catch (Exception mess)
            {
                MessageBox.Show(mess.ToString());
                return false;
            }

            return true;
        }

        public bool OpenExcelFile()
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return false;
            }
            FileInfo fi = new FileInfo(filePath);
            if (fi.Exists)
            {
                System.Diagnostics.Process.Start(filePath);
                return true;
            }
            return false;
        }
    }
}