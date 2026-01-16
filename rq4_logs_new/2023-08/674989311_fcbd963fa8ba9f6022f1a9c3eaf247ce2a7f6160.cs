using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Windows.Forms;
using Microsoft.Office.Interop.Excel;

namespace DataLoggerAppV1
{
    internal class WriteExcel
    {
        public static void writeExcel(string[] Infor)
        {
            const string MyFileName = "ExcelTemplate.xlsx";
            string execPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().CodeBase);
            var filePath = Path.Combine(execPath, MyFileName);
            Microsoft.Office.Interop.Excel.Application excel = new Microsoft.Office.Interop.Excel.Application();
            Workbook wb;
            Worksheet ws;

            wb = excel.Workbooks.Open(filePath);
            ws = (Microsoft.Office.Interop.Excel.Worksheet)wb.Sheets[3];

            Range celRange = ws.Range["D2:G2"];
            string[] CustomerInfor = Infor;
            
            celRange.set_Value(XlRangeValueDataType.xlRangeValueDefault, CustomerInfor);
            


            try
            {
                wb.SaveAs("C:\\Users\\Admin\\Downloads\\Certificate.xlsx");
                wb.Close();

                Process.Start("C:\\Users\\Admin\\Downloads\\Certificate.xlsx");
                excel.Quit();
            }
            catch (Exception e)
            {
                MessageBox.Show("Some Excel files is Opened, Please close excel files and try again!");
            }
            
        }
    }
}