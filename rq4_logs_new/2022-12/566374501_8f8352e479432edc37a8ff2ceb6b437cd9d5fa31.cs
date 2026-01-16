using ExcelDataReader;
using Standard.Tool.Platform.Common.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Data.Infrastructure.Services
{
    public class ReadTableService : IReadTableService
    {
        public DataTable ReadCsv(Stream stream)
        {
            using (var reader = ExcelReaderFactory.CreateCsvReader(stream))
            {
                DataSet ds = reader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    UseColumnDataType = false,
                    ConfigureDataTable = reader => new ExcelDataTableConfiguration() { UseHeaderRow = true }
                });

                return ds.Tables.Count == 0 ? null : NormalizeColumnName(ds.Tables[0]);
            }
        }

        public DataSet ReadExcel()
        {
            var filePath = ExcelHelper.OpenExcelFileDialog();
            if (!string.IsNullOrEmpty(filePath))
            {
                try
                {
                    using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                    {
                        using (var reader = ExcelReaderFactory.CreateReader(fileStream))
                        {
                            DataSet ds = reader.AsDataSet(new ExcelDataSetConfiguration()
                            {
                                UseColumnDataType = false,
                                ConfigureDataTable = reader => new ExcelDataTableConfiguration() { UseHeaderRow = true }
                            });

                            return ds;
                        }
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
            return null;
        }

        private DataTable NormalizeColumnName(DataTable table)
        {
            foreach (DataColumn col in table.Columns)
            {
                col.ColumnName = NormalizeString(col.ColumnName);
            }

            return table;
        }

        public string NormalizeString(string columnName)
        {
            string result = "";
            columnName = columnName.Trim();
            bool begin = false;

            foreach (var c in columnName)
            {
                if (!begin && char.IsDigit(c))
                {
                    result += '_';
                }
                else
                {
                    if (c == '_' || char.IsLetterOrDigit(c))
                    {
                        result += c;
                    }
                    else
                    {
                        result += '_';
                    }
                }

                begin = true;
            }

            return result.ToLowerInvariant();
        }
    }
}