using System;
using System.Collections.ObjectModel;
using System.Linq;
using HighFreqUpdate.Helpers;
using Infragistics.Windows.DataPresenter;

namespace HighFreqUpdate.Models
{
    public class ManagePropertiesModel
    {
        private XamDataGrid grid;

        internal class Resources
        {
            public const string DateFormatddMMYYYY = "dd/MM/yyyy";
            public const string DateFormatMMddYYYY = "MM/dd/yyyy";
            public const string DateFormatMMYYYY = "MM/yyyy";
            public const string DateFormatYYYYMM = "yyyy/MM";
            public const string DateFormatddMMyyyyHHmmss = "dd/MM/yyyy HH:mm:ss";
            public const string DateFormatMMddyyyyHHmmss = "MM/dd/yyyy HH:mm:ss";

            public const string LeftAlignment = "Sinistra";
            public const string CenterAlignment = "Centro";
            public const string RightAlignment = "Destra";
            public const string JustifyAlignment = "Giustificato";

            public const string SumAggregator = "Somma";
            public const string AverageAggregator = "Media";
            public const string TotalRowsAggregator = "Totale righe";
        }
        public ObservableCollection<ColumnItem> Columns { get; set; }

        public ObservableCollection<FormatItem> Formats { get; set; }

        public ObservableCollection<AlignItem> Alignments { get; set; }

        public ObservableCollection<AggregatorItem> Aggregators { get; set; }

        public XamDataGrid Grid
        {
            get => grid;
            set
            {
                grid = value;
                SetColumnsInformation();
            }
        }

        public string SelectedColumn { get; set; }

        public ManagePropertiesModel()
        {
            Columns = new ObservableCollection<ColumnItem>();

            Formats = new ObservableCollection<FormatItem>
            {
                new FormatItem {FormatId = 0, FormatName = null},
                new FormatItem {FormatId = 1, FormatName = Resources.DateFormatddMMYYYY},
                new FormatItem {FormatId = 2, FormatName = Resources.DateFormatMMddYYYY},
                new FormatItem {FormatId = 3, FormatName = Resources.DateFormatMMYYYY},
                new FormatItem {FormatId = 4, FormatName = Resources.DateFormatYYYYMM},
                new FormatItem {FormatId = 5, FormatName = Resources.DateFormatddMMyyyyHHmmss},
                new FormatItem {FormatId = 6, FormatName = Resources.DateFormatMMddyyyyHHmmss}
            };

            Alignments = new ObservableCollection<AlignItem>
            {
                new AlignItem {AlignId = 0, AlignName = Resources.LeftAlignment},
                new AlignItem {AlignId = 1, AlignName = Resources.CenterAlignment},
                new AlignItem {AlignId = 2, AlignName = Resources.RightAlignment}
            };

            Aggregators = new ObservableCollection<AggregatorItem>
            {
                new AggregatorItem {AggregatorId = 0, AggregatorName = null},
                new AggregatorItem {AggregatorId = 1, AggregatorName = Resources.SumAggregator}
            };
        }

        protected void SetColumnsInformation()
        {
            if (Grid?.FieldLayouts != null)
            {
                var fields = Grid.FieldLayouts.First().Fields;

                foreach (var field in fields)
                {
                    Columns.Add(
                        new ColumnItem
                        {
                            //Name = (string)field.Label,
                            //Type = field.DataType,
                            //Decimals = GetDecimals(field),
                            //EnableThousandSeparator = field.Format?.Contains("N") ?? false,
                            //EnableCheckThousandSeparator = CheckThousandSeparator(field),
                            //Format = GetFormatType(field),
                            //Align = GetAligment(field),
                            //EnableAggregator = field.AggregateFunctions.Count > 0,
                            //Aggregator = GetAggregator(field),
                            //EnableFreezeColumn = field.DisplayIndex < Grid.FrozenColumnCount,
                            //EnableLockColumn = !field.IsReorderable,
                            //CanBlink = field.DataType != typeof(string) && !GridCustomizationHelpers.DateTimeTypes.Contains(field.DataType),
                            //CanChangeBlinkColor = (string)field.Tag == TagBlinkable,
                            //IsBlinking = GridColumnProperties.GetIsBlinking(field),
                            //BlinkColor = GridColumnProperties.GetBlinkColor(field),
                            //BlinkingFrequency = GridColumnProperties.GetBlinkFreq(field),
                            //IsForeColorNegativeEnabled = GridColumnProperties.GetIsFontColorNegativeEnabled(field),
                            //ForeColorNegative = GridColumnProperties.GettForeColorNegative(field),
                        });
                }
            }
        }

        private int GetDecimals(Field column)
        {
            int res = !string.IsNullOrEmpty(column.Format) && IsNumberType(column.DataType) ? Convert.ToInt32(column.Format.Substring(1, 1)) : 0;

            return res;
        }

        private bool CheckThousandSeparator(Field column)
        {
            return string.IsNullOrEmpty(column.Format) && (column.Format.Contains("N") || column.Format.Contains("F"));
        }

        private int GetFormatType(Field column)
        {
            return string.IsNullOrEmpty(column.Format) && GridCustomizationHelpers.DateTimeTypes.Contains(column.DataType) ? Formats.First(x => x.FormatName == column.Format).FormatId : Formats.First().FormatId;
        }

        private int GetAligment(Field column)
        {
            var key = column.HorizontalContentAlignment.HasValue ? (int)column.HorizontalContentAlignment.Value : -1;
            if (!GridCustomizationHelpers.MappingTextAligment.ContainsKey(key)) throw new Exception();//throw new KeyNotFoundException(key.ToString());

            return GridCustomizationHelpers.MappingTextAligment[key];
        }

        private int GetAggregator(Field column)
        {
            //if (column.AggregateFunctions.Count == 0) return 0;

            //if (column.AggregateFunctions["Count"] != null)
            //{
            //    return Aggregators.First(x => x.AggregatorName == Resources.TotalRowsAggregator).AggregatorId;
            //}
            return 0;
        }

        private bool IsNumberType(Type type)
        {
            return GridCustomizationHelpers.NumericTypes.Contains(type);
        }
    }
}