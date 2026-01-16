using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Common.Helper
{
    public class ExcelHelper
    {
        #region 01，保存excel对话框返回文件名
        public static string SaveExcelFileDialog()
        {
            var sfd = new Microsoft.Win32.SaveFileDialog()
            {
                DefaultExt = "xls",
                Filter = "excel files(*.xls)|*.xls|All files(*.*)|*.*",
                FilterIndex = 1
            };

            if (sfd.ShowDialog() != true)
                return null;
            return sfd.FileName;
        }
        #endregion

        #region 02，打开excel对话框返回文件名
        public static string OpenExcelFileDialog()
        {
            var ofd = new Microsoft.Win32.OpenFileDialog()
            {
                DefaultExt = "xls",
                Filter = "excel files(*.xlsx)|*.xlsx|All files(*.*)|*.*",
                FilterIndex = 1
            };

            if (ofd.ShowDialog() != true)
                return null;
            return ofd.FileName;
        }
        #endregion

        #region 03，读excel
        public static FileStream ImportExcelFile(string filePath)
        {
            DataTable dt = new DataTable();

            if (filePath != null)
            {
                return new FileStream(filePath, FileMode.Open, FileAccess.Read);
            }
            return null;
        }
        #endregion

        #region 04，list转datatable
        public static DataTable ListToDataTable<T>(IEnumerable<T> c)
        {
            var props = typeof(T).GetProperties();
            var dt = new DataTable();
            dt.Columns.AddRange(props.Select(p => new DataColumn(p.Name, p.PropertyType)).ToArray());
            if (c.Count() > 0)
            {
                for (int i = 0; i < c.Count(); i++)
                {
                    ArrayList tempList = new ArrayList();
                    foreach (PropertyInfo item in props)
                    {
                        object obj = item.GetValue(c.ElementAt(i), null);
                        tempList.Add(obj);
                    }
                    dt.LoadDataRow(tempList.ToArray(), true);
                }
            }
            return dt;
        }
        #endregion

        #region 05，写入excel
        public static bool WriteExcel<T>(IList<T> list)
        {

            //打开保存excel对话框
            var filepath = SaveExcelFileDialog();
            if (filepath == null)
                return false;

            var dt = ListToDataTable<T>(list);

            if (!string.IsNullOrEmpty(filepath) && null != dt && dt.Rows.Count > 0)
            {
                NPOI.HSSF.UserModel.HSSFWorkbook book = new NPOI.HSSF.UserModel.HSSFWorkbook();
                NPOI.SS.UserModel.ISheet sheet = book.CreateSheet("Sheet1");

                NPOI.SS.UserModel.IRow row = sheet.CreateRow(0);
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    row.CreateCell(i).SetCellValue(dt.Columns[i].ColumnName);
                }
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    NPOI.SS.UserModel.IRow row2 = sheet.CreateRow(i + 1);
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        row2.CreateCell(j).SetCellValue(Convert.ToString(dt.Rows[i][j]));
                    }
                }
                // 写入到客户端  
                using (System.IO.MemoryStream ms = new System.IO.MemoryStream())
                {
                    book.Write(ms);
                    using (FileStream fs = new FileStream(filepath, FileMode.Create, FileAccess.Write))
                    {
                        byte[] data = ms.ToArray();
                        fs.Write(data, 0, data.Length);
                        fs.Flush();
                    }
                    book = null;
                }
            }
            return true;
        }
        #endregion
    }
}