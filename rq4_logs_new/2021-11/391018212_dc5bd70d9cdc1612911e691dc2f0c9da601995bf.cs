
namespace FeedSleepRepeatUI
{
    partial class NappyChart
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.Windows.Forms.DataVisualization.Charting.ChartArea chartArea1 = new System.Windows.Forms.DataVisualization.Charting.ChartArea();
            System.Windows.Forms.DataVisualization.Charting.Legend legend1 = new System.Windows.Forms.DataVisualization.Charting.Legend();
            System.Windows.Forms.DataVisualization.Charting.Series series1 = new System.Windows.Forms.DataVisualization.Charting.Series();
            System.Windows.Forms.DataVisualization.Charting.Series series2 = new System.Windows.Forms.DataVisualization.Charting.Series();
            System.Windows.Forms.DataVisualization.Charting.Series series3 = new System.Windows.Forms.DataVisualization.Charting.Series();
            this.nappiesChart = new System.Windows.Forms.DataVisualization.Charting.Chart();
            ((System.ComponentModel.ISupportInitialize)(this.nappiesChart)).BeginInit();
            this.SuspendLayout();
            // 
            // nappiesChart
            // 
            chartArea1.Name = "ChartArea1";
            this.nappiesChart.ChartAreas.Add(chartArea1);
            legend1.Name = "Legend1";
            this.nappiesChart.Legends.Add(legend1);
            this.nappiesChart.Location = new System.Drawing.Point(12, 12);
            this.nappiesChart.Name = "nappiesChart";
            this.nappiesChart.Palette = System.Windows.Forms.DataVisualization.Charting.ChartColorPalette.SemiTransparent;
            series1.ChartArea = "ChartArea1";
            series1.Legend = "Legend1";
            series1.Name = "Wet Nappies";
            series2.ChartArea = "ChartArea1";
            series2.Legend = "Legend1";
            series2.Name = "Dirty Nappies";
            series3.ChartArea = "ChartArea1";
            series3.Legend = "Legend1";
            series3.Name = "Total";
            this.nappiesChart.Series.Add(series1);
            this.nappiesChart.Series.Add(series2);
            this.nappiesChart.Series.Add(series3);
            this.nappiesChart.Size = new System.Drawing.Size(776, 426);
            this.nappiesChart.TabIndex = 0;
            this.nappiesChart.Text = "chart1";
            // 
            // DayGraph
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.nappiesChart);
            this.Name = "DayGraph";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Day Graph";
            this.Load += new System.EventHandler(this.DayGraph_Load);
            ((System.ComponentModel.ISupportInitialize)(this.nappiesChart)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.DataVisualization.Charting.Chart nappiesChart;
    }
}