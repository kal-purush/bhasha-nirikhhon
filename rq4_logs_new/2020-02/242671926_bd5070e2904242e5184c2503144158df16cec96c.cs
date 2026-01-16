using Microsoft.Win32;
using Syncfusion.SfSkinManager;
using Syncfusion.Windows.Controls.Grid;
using Syncfusion.Windows.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace Tooltip
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            SfSkinManager.SetVisualStyle(this, VisualStyles.Office2016Colorful);
            InitializeComponent();
            GridControl gridcontrol = new GridControl();
            
            gridcontrol.Width = 820;
            gridcontrol.Height = 470;

            gridcontrol.Model.RowCount = 20;
            gridcontrol.Model.ColumnCount = 10;
            gridcontrol.QueryCellInfo += Gridcontrol_QueryCellInfo;

            //Enable the show tooltip
            GridTooltipService.SetShowTooltips(gridcontrol, true);
            
            //Set the delay for tooltip
            //GridTooltipService.SetTooltipDelay(gridcontrol, 5000);

            // Change the header style.
            gridcontrol.Model.HeaderStyle.Background = Brushes.White;
            gridcontrol.Model.HeaderStyle.Borders.All = new System.Windows.Media.Pen(System.Windows.Media.Brushes.LightGray, 1);
    
            //CellToolTipOpening event
            gridcontrol.CellToolTipOpening += Gridcontrol_CellToolTipOpening;

            //Set tooltip for specific cell
            gridcontrol.Model[1, 1].ToolTip = " Grid (" + gridcontrol.Model[1, 1].CellValue + ") ";
            gridcontrol.Model[1, 1].ShowTooltip = true;
            gridcontrol.Model[1, 1].Enabled = false;

            //Access the datatemplate for tooltip using ToolTipTemplate
            gridcontrol.Model[1, 1].TooltipTemplate = (DataTemplate)this.Resources["tooltipTemplate1"];

            ////Access the datatemplate for tooltip using ToolTipTemplateKey
            //gridcontrol.Model[1, 1].TooltipTemplateKey = "tooltipTemplate1";

            //Set tooltip for header
            gridcontrol.Model.RowStyles[0].ToolTip = "Hello";
            gridcontrol.Model.RowStyles[0].ShowTooltip = true;
                
            //Set tooltip for specific row
            gridcontrol.Model.RowStyles[1].ToolTip = " First row ";
            gridcontrol.Model.RowStyles[1].ShowTooltip = true;

            //Set tooltip for specific column
            gridcontrol.Model.ColStyles[1].ToolTip = " First column ";
            gridcontrol.Model.ColStyles[1].ShowTooltip = true;

            ////Reset the tooltip values for cell or row or column
            //gridcontrol.Model[1, 1].ResetValue(GridStyleInfoStore.ToolTipProperty);
            //gridcontrol.Model.RowStyles[1].ResetValue(GridStyleInfoStore.ToolTipProperty);
            //gridcontrol.Model.ColStyles[1].ResetValue(GridStyleInfoStore.ToolTipProperty);
            grid.Children.Add(gridcontrol);
        }

        private void Gridcontrol_CellToolTipOpening(object sender, GridCellToolTipOpeningEventArgs e)
        {
            var grids = sender as GridControl;
            if (e.Cell.RowIndex == 1 && e.Cell.ColumnIndex == 1)
                grids.Model[e.Cell.RowIndex, e.Cell.ColumnIndex].ToolTip = "Hello";
        }

        //Set tooltip using QueryCellInfo event
        private void Gridcontrol_QueryCellInfo(object sender, GridQueryCellInfoEventArgs e)
        {
            e.Style.CellValue = string.Format("{0},{1}", e.Cell.RowIndex, e.Cell.ColumnIndex);

            //Access the datatemplate for tooltip using ToolTipTemplate
            //e.Style.TooltipTemplate = (DataTemplate)this.Resources["tooltipTemplate1"];
            //e.Style.ShowTooltip = true;
            //e.Style.TooltipTemplateKey = "tooltipTemplate1";

            ////Show tooltip for a specific index
            //if (e.Cell.RowIndex == 1 && e.Cell.ColumnIndex == 1)
            //    e.Style.ToolTip = " Grid (" + e.Cell.RowIndex + "," + e.Cell.ColumnIndex + ") ";
            //else
            //    e.Style.ToolTip = e.Style.CellValue;

            ////Show tooltip for row.
            //if (e.Cell.ColumnIndex > 0 && e.Cell.RowIndex == 5)
            //        e.Style.ToolTip = " Row " + "(" + e.Cell.RowIndex + "," + e.Cell.ColumnIndex + ") ";

            //// Show tooltip for column.
            //if (e.Cell.RowIndex > 0 && e.Cell.ColumnIndex == 4)
            //    e.Style.ToolTip = " Column " + "(" + e.Cell.RowIndex + "," + e.Cell.ColumnIndex + ") ";

            ////highlight cell has a ToolTip
            //if (e.Style.HasToolTip)
            //{
            //    e.Style.Background = Brushes.Green;
            //    e.Style.Foreground = Brushes.White;
            //}
        }
    }
}