using System;
using System.Linq;

namespace SoftmakeAll.SDK.FileWR.CSV
{
  public class ConverterEngine
  {
    #region Constructor
    public ConverterEngine()
    {
      this.Encoding = System.Text.Encoding.UTF8;
      this.ContainsHeader = true;
      this.SpecificColumnsCultureInfo = new System.Collections.Generic.List<SoftmakeAll.SDK.FileWR.CSV.SpecificColumnCultureInfo>();

      this.AllowedDataTypeNameReplacements = new System.Collections.Generic.List<System.Tuple<System.String, System.String>>();
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Boolean", "Int32"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Boolean", "Int64"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Boolean", "Single"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Boolean", "Double"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Int32", "Int64"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Int32", "Single"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Int32", "Double"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Int64", "Double"));
      this.AllowedDataTypeNameReplacements.Add(new System.Tuple<System.String, System.String>("Single", "Double"));
    }
    #endregion

    #region Properties
    public System.String FileName { get; set; }
    public System.Text.Encoding Encoding { get; set; }
    public System.Boolean ContainsHeader { get; set; }
    public System.Globalization.CultureInfo CultureInfo { get; set; }
    public System.Collections.Generic.List<SoftmakeAll.SDK.FileWR.CSV.SpecificColumnCultureInfo> SpecificColumnsCultureInfo { get; }
    #endregion

    #region Fields
    private System.Collections.Generic.List<System.Tuple<System.String, System.String>> AllowedDataTypeNameReplacements;
    #endregion

    #region Methods
    public async System.Threading.Tasks.Task<System.Data.DataTable> ConvertToDataTableAsync()
    {
      if (!(System.IO.File.Exists(FileName)))
        throw new System.Exception("File not found.");

      System.Data.DataTable Result = new System.Data.DataTable();
      System.Collections.Generic.List<SoftmakeAll.SDK.FileWR.CSV.FileColumn> FileColumns = new System.Collections.Generic.List<SoftmakeAll.SDK.FileWR.CSV.FileColumn>();
      const System.String RegexSplit = "(?:,|\\n|^)(\"(?:(?:\"\")*[^\"]*)*\"|[^\",\\n]*|(?:\\n|$))";

      using System.IO.FileStream FileStream = new System.IO.FileStream(this.FileName, System.IO.FileMode.Open, System.IO.FileAccess.Read);

      using (System.IO.StreamReader AnalyserStreamReader = new System.IO.StreamReader(FileStream, this.Encoding, false, 4096, true))
      {
        System.String CurrentLine = null;
        while (AnalyserStreamReader.Peek() > -1)
        {
          CurrentLine = await AnalyserStreamReader.ReadLineAsync();
          if (!(System.String.IsNullOrWhiteSpace(CurrentLine)))
            break;
        }
        if (System.String.IsNullOrWhiteSpace(CurrentLine)) return null;

        System.String[] DataTableColumnNames = System.Text.RegularExpressions.Regex.Split(CurrentLine, RegexSplit).Where((c, i) => i % 2 == 1).ToArray();
        for (System.Int32 i = 0; i < DataTableColumnNames.Length; i++)
          FileColumns.Add(new SoftmakeAll.SDK.FileWR.CSV.FileColumn() { Name = ((!(this.ContainsHeader)) ? $"column{i:00000}" : DataTableColumnNames[i]), Index = i, AllowedDataTypeNameReplacements = AllowedDataTypeNameReplacements });
        DataTableColumnNames = null;

        while (AnalyserStreamReader.Peek() > -1)
        {
          CurrentLine = await AnalyserStreamReader.ReadLineAsync();
          if (System.String.IsNullOrWhiteSpace(CurrentLine))
            continue;

          System.String[] Columns = System.Text.RegularExpressions.Regex.Split(CurrentLine, RegexSplit).Where((c, i) => i % 2 == 1).ToArray();
          System.Int32 MaxColumns = System.Math.Min(FileColumns.Count, Columns.Length);
          for (System.Int32 i = 0; i < MaxColumns; i++)
          {
            if ((Columns[i].StartsWith('\"')) && (Columns[i].EndsWith('\"')))
              Columns[i] = Columns[i][1..^1];

            FileColumns[i].AllowNulls = System.String.IsNullOrEmpty(Columns[i]);
            if (!(System.String.IsNullOrEmpty(Columns[i])))
              FileColumns[i].MaxLength = Columns[i].Length;
            else
              continue;

            if ((this.CultureInfo == null) && (this.SpecificColumnsCultureInfo.Count == 0))
              continue;

            this.DefineFileColumnProperties(FileColumns[i], Columns[i]);
          }
        }

        foreach (SoftmakeAll.SDK.FileWR.CSV.FileColumn FileColumn in FileColumns)
        {
          if (System.String.IsNullOrWhiteSpace(FileColumn.DataTypeName))
            FileColumn.DataTypeName = "String";
          Result.Columns.Add(new System.Data.DataColumn(FileColumn.Name, System.Type.GetType($"System.{FileColumn.DataTypeName}", true, false)) { MaxLength = FileColumn.MaxLength });
        }
      }

      FileStream.Position = 0;
      using (System.IO.StreamReader AnalyserStreamReader = new System.IO.StreamReader(FileStream, this.Encoding))
      {
        if (this.ContainsHeader) await AnalyserStreamReader.ReadLineAsync();
        System.String CurrentLine = null;
        while (AnalyserStreamReader.Peek() > -1)
        {
          CurrentLine = await AnalyserStreamReader.ReadLineAsync();
          if (System.String.IsNullOrWhiteSpace(CurrentLine))
            continue;

          System.Data.DataRow DataRow = Result.Rows.Add();
          System.String[] Columns = System.Text.RegularExpressions.Regex.Split(CurrentLine, RegexSplit).Where((c, i) => i % 2 == 1).ToArray();
          System.Int32 MaxColumns = System.Math.Min(FileColumns.Count, Columns.Length);
          for (System.Int32 i = 0; i < MaxColumns; i++)
            if (System.String.IsNullOrEmpty(Columns[i]))
              DataRow[i] = System.Convert.DBNull;
            else
            {
              if ((Columns[i].StartsWith('\"')) && (Columns[i].EndsWith('\"')))
                Columns[i] = Columns[i][1..^1];

              if ((this.CultureInfo == null) && (this.SpecificColumnsCultureInfo.Count == 0))
              {
                DataRow[i] = System.Net.WebUtility.HtmlDecode(Columns[i]);
                continue;
              }

              System.Globalization.CultureInfo ConverterFormatProvider = this.GetConverterFormatProvider(i);
              System.String NumberGroupSeparator = ConverterFormatProvider.NumberFormat.NumberGroupSeparator;

              switch (FileColumns[i].DataTypeName)
              {
                case "Boolean":
                  DataRow[i] = System.Int32.Parse(Columns[i].Replace("true", "1").Replace("false", "0"));
                  break;
                case "DateTime":
                  DataRow[i] = System.Convert.ToDateTime(Columns[i], ConverterFormatProvider);
                  break;
                case "Int32":
                  DataRow[i] = System.Convert.ToInt32(System.Text.RegularExpressions.Regex.Replace(Columns[i], @$"[\-\+\{NumberGroupSeparator}]", ""), ConverterFormatProvider);
                  break;
                case "Int64":
                  DataRow[i] = System.Convert.ToInt64(System.Text.RegularExpressions.Regex.Replace(Columns[i], @$"[\-\+\{NumberGroupSeparator}]", ""), ConverterFormatProvider);
                  break;
                case "Single":
                  DataRow[i] = System.Convert.ToSingle(Columns[i], ConverterFormatProvider);
                  break;
                case "Double":
                  DataRow[i] = System.Convert.ToDouble(Columns[i], ConverterFormatProvider);
                  break;
                default:
                  DataRow[i] = System.Net.WebUtility.HtmlDecode(Columns[i]);
                  break;
              }
            }
        }
      }

      for (System.Int32 i = 0; i < Result.Columns.Count; i++)
        Result.Columns[i].AllowDBNull = FileColumns[i].AllowNulls;

      return Result;
    }
    private void DefineFileColumnProperties(SoftmakeAll.SDK.FileWR.CSV.FileColumn FileColumn, System.String Value)
    {
      Value = Value.ToLower();

      if ((Value == "0") || (Value == "1") || (Value == "false") || (Value == "true"))
      {
        FileColumn.DataTypeName = "Boolean";
        return;
      }

      System.Globalization.CultureInfo ConverterFormatProvider = this.GetConverterFormatProvider(FileColumn.Index);

      if ((Value.Length == 10) && (Value.Count(c => ((c == '/') || (c == '-'))) == 2))
      {
        System.DateTime Parsed = new System.DateTime();
        if (!(System.DateTime.TryParse(Value, CultureInfo, System.Globalization.DateTimeStyles.None, out Parsed)))
        {
          FileColumn.DataTypeName = "String";
          return;
        }
        FileColumn.DataTypeName = "DateTime";
        return;
      }

      if ((Value.Any(c => System.Char.IsLetter(c))) || ((System.String.IsNullOrWhiteSpace(Value)) && (Value != "")))
      {
        FileColumn.DataTypeName = "String";
        return;
      }

      System.String NumberGroupSeparator = ConverterFormatProvider.NumberFormat.NumberGroupSeparator;
      if (System.Text.RegularExpressions.Regex.Replace(Value, @$"[\-\+\{NumberGroupSeparator}]", "").All(c => System.Char.IsNumber(c)))
      {
        System.Int64 Parsed = 0;
        if (!(System.Int64.TryParse(Value, System.Globalization.NumberStyles.Any, ConverterFormatProvider, out Parsed)))
        {
          FileColumn.DataTypeName = "String";
          return;
        }
        if (Parsed > System.Int32.MaxValue)
          FileColumn.DataTypeName = "Int64";
        else
          FileColumn.DataTypeName = "Int32";
        return;
      }

      if ((System.Text.RegularExpressions.Regex.Replace(Value, @"[\-\+\.\,]", "").All(c => System.Char.IsNumber(c))))
      {
        System.Double Parsed = 0;
        if (!(System.Double.TryParse(Value, System.Globalization.NumberStyles.Any, ConverterFormatProvider, out Parsed)))
        {
          FileColumn.DataTypeName = "String";
          return;
        }
        if (Parsed > System.Single.MaxValue)
          FileColumn.DataTypeName = "Double";
        else
          FileColumn.DataTypeName = "Single";
        return;
      }

      FileColumn.DataTypeName = "String";
    }
    private System.Globalization.CultureInfo GetConverterFormatProvider(System.Int32 ColumnIndex)
    {
      return this.SpecificColumnsCultureInfo.FirstOrDefault(c => c.ColumnIndex == ColumnIndex)?.CultureInfo ?? this.CultureInfo;
    }
    #endregion
  }
}