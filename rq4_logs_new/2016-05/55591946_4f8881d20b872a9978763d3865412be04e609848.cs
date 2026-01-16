using System;
using System.Collections;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Windows.Forms;
using System.Windows.Forms.DataVisualization.Charting;
using MetroFramework.Controls;

namespace WinFormsChartSamples {
    /// <summary>
    /// Summary description for LineCurvesChart.
    /// </summary>
    public class LineCurvesChart : MetroUserControl {
        private System.Windows.Forms.DataVisualization.Charting.Chart chart1;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.CheckBox checkBoxShowMargin;
        private System.Windows.Forms.CheckBox checkBoxShow3D;
        private TableLayoutPanel tableLayoutPanel1;
        private MetroLabel captionLabel;

        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.Container components = null;

        public LineCurvesChart() {
            // This call is required by the Windows.Forms Form Designer.
            InitializeComponent();

            // TODO: Add any initialization after the InitForm call

        }

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        protected override void Dispose(bool disposing) {
            if (disposing) {
                if (components != null) {
                    components.Dispose();
                }
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            System.Windows.Forms.DataVisualization.Charting.ChartArea chartArea1 = new System.Windows.Forms.DataVisualization.Charting.ChartArea();
            System.Windows.Forms.DataVisualization.Charting.Legend legend1 = new System.Windows.Forms.DataVisualization.Charting.Legend();
            System.Windows.Forms.DataVisualization.Charting.Series series1 = new System.Windows.Forms.DataVisualization.Charting.Series();
            System.Windows.Forms.DataVisualization.Charting.Series series2 = new System.Windows.Forms.DataVisualization.Charting.Series();
            this.chart1 = new System.Windows.Forms.DataVisualization.Charting.Chart();
            this.panel1 = new System.Windows.Forms.Panel();
            this.checkBoxShow3D = new System.Windows.Forms.CheckBox();
            this.checkBoxShowMargin = new System.Windows.Forms.CheckBox();
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.captionLabel = new MetroFramework.Controls.MetroLabel();
            ((System.ComponentModel.ISupportInitialize)(this.chart1)).BeginInit();
            this.panel1.SuspendLayout();
            this.tableLayoutPanel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // chart1
            // 
            this.chart1.BackGradientStyle = System.Windows.Forms.DataVisualization.Charting.GradientStyle.TopBottom;
            this.chart1.BorderlineColor = System.Drawing.Color.DeepSkyBlue;
            this.chart1.BorderlineDashStyle = System.Windows.Forms.DataVisualization.Charting.ChartDashStyle.Solid;
            this.chart1.BorderlineWidth = 2;
            this.chart1.BorderSkin.SkinStyle = System.Windows.Forms.DataVisualization.Charting.BorderSkinStyle.Emboss;
            chartArea1.Area3DStyle.Inclination = 40;
            chartArea1.Area3DStyle.IsClustered = true;
            chartArea1.Area3DStyle.IsRightAngleAxes = false;
            chartArea1.Area3DStyle.LightStyle = System.Windows.Forms.DataVisualization.Charting.LightStyle.Realistic;
            chartArea1.Area3DStyle.Perspective = 9;
            chartArea1.Area3DStyle.Rotation = 25;
            chartArea1.Area3DStyle.WallWidth = 3;
            chartArea1.AxisX.LabelStyle.Font = new System.Drawing.Font("Trebuchet MS", 8.25F, System.Drawing.FontStyle.Bold);
            chartArea1.AxisX.LineColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            chartArea1.AxisX.MajorGrid.LineColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            chartArea1.AxisY.LabelStyle.Font = new System.Drawing.Font("Trebuchet MS", 8.25F, System.Drawing.FontStyle.Bold);
            chartArea1.AxisY.LineColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            chartArea1.AxisY.MajorGrid.LineColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            chartArea1.BackColor = System.Drawing.Color.OldLace;
            chartArea1.BackGradientStyle = System.Windows.Forms.DataVisualization.Charting.GradientStyle.TopBottom;
            chartArea1.BackSecondaryColor = System.Drawing.Color.White;
            chartArea1.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            chartArea1.BorderDashStyle = System.Windows.Forms.DataVisualization.Charting.ChartDashStyle.Solid;
            chartArea1.Name = "Default";
            chartArea1.ShadowColor = System.Drawing.Color.Transparent;
            this.chart1.ChartAreas.Add(chartArea1);
            this.chart1.Dock = System.Windows.Forms.DockStyle.Fill;
            legend1.BackColor = System.Drawing.Color.Transparent;
            legend1.Enabled = false;
            legend1.Font = new System.Drawing.Font("Trebuchet MS", 8.25F, System.Drawing.FontStyle.Bold);
            legend1.IsTextAutoFit = false;
            legend1.Name = "Default";
            this.chart1.Legends.Add(legend1);
            this.chart1.Location = new System.Drawing.Point(3, 24);
            this.chart1.Name = "chart1";
            this.chart1.Palette = System.Windows.Forms.DataVisualization.Charting.ChartColorPalette.Bright;
            series1.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(180)))), ((int)(((byte)(26)))), ((int)(((byte)(59)))), ((int)(((byte)(105)))));
            series1.BorderWidth = 3;
            series1.ChartArea = "Default";
            series1.ChartType = System.Windows.Forms.DataVisualization.Charting.SeriesChartType.Line;
            series1.Color = System.Drawing.Color.FromArgb(((int)(((byte)(220)))), ((int)(((byte)(65)))), ((int)(((byte)(140)))), ((int)(((byte)(240)))));
            series1.Legend = "Default";
            series1.MarkerSize = 8;
            series1.MarkerStyle = System.Windows.Forms.DataVisualization.Charting.MarkerStyle.Circle;
            series1.Name = "Series1";
            series1.ShadowColor = System.Drawing.Color.Black;
            series1.ShadowOffset = 2;
            series1.XValueType = System.Windows.Forms.DataVisualization.Charting.ChartValueType.Double;
            series1.YValueType = System.Windows.Forms.DataVisualization.Charting.ChartValueType.Double;
            series2.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(180)))), ((int)(((byte)(26)))), ((int)(((byte)(59)))), ((int)(((byte)(105)))));
            series2.BorderWidth = 3;
            series2.ChartArea = "Default";
            series2.ChartType = System.Windows.Forms.DataVisualization.Charting.SeriesChartType.Line;
            series2.Color = System.Drawing.Color.FromArgb(((int)(((byte)(220)))), ((int)(((byte)(224)))), ((int)(((byte)(64)))), ((int)(((byte)(10)))));
            series2.Legend = "Default";
            series2.MarkerSize = 9;
            series2.MarkerStyle = System.Windows.Forms.DataVisualization.Charting.MarkerStyle.Diamond;
            series2.Name = "Series2";
            series2.ShadowColor = System.Drawing.Color.Black;
            series2.ShadowOffset = 2;
            series2.XValueType = System.Windows.Forms.DataVisualization.Charting.ChartValueType.Double;
            series2.YValueType = System.Windows.Forms.DataVisualization.Charting.ChartValueType.Double;
            this.chart1.Series.Add(series1);
            this.chart1.Series.Add(series2);
            this.chart1.Size = new System.Drawing.Size(457, 291);
            this.chart1.TabIndex = 1;
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.checkBoxShow3D);
            this.panel1.Controls.Add(this.checkBoxShowMargin);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(3, 321);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(457, 31);
            this.panel1.TabIndex = 2;
            // 
            // checkBoxShow3D
            // 
            this.checkBoxShow3D.CheckAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.checkBoxShow3D.Location = new System.Drawing.Point(187, 3);
            this.checkBoxShow3D.Name = "checkBoxShow3D";
            this.checkBoxShow3D.Size = new System.Drawing.Size(168, 24);
            this.checkBoxShow3D.TabIndex = 5;
            this.checkBoxShow3D.Text = "Display chart as 3&D:";
            this.checkBoxShow3D.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.checkBoxShow3D.CheckedChanged += new System.EventHandler(this.checkBoxShow3D_CheckedChanged);
            // 
            // checkBoxShowMargin
            // 
            this.checkBoxShowMargin.CheckAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.checkBoxShowMargin.Location = new System.Drawing.Point(3, 3);
            this.checkBoxShowMargin.Name = "checkBoxShowMargin";
            this.checkBoxShowMargin.Size = new System.Drawing.Size(168, 24);
            this.checkBoxShowMargin.TabIndex = 4;
            this.checkBoxShowMargin.Text = "Show X Axis &Margin:";
            this.checkBoxShowMargin.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.checkBoxShowMargin.CheckedChanged += new System.EventHandler(this.checkBoxShowMargin_CheckedChanged);
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.chart1, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.panel1, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.captionLabel, 0, 0);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.RowCount = 3;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 21F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 37F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(463, 355);
            this.tableLayoutPanel1.TabIndex = 3;
            // 
            // captionLabel
            // 
            this.captionLabel.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.captionLabel.AutoSize = true;
            this.captionLabel.Location = new System.Drawing.Point(3, 0);
            this.captionLabel.Name = "captionLabel";
            this.captionLabel.Size = new System.Drawing.Size(457, 19);
            this.captionLabel.TabIndex = 3;
            this.captionLabel.Text = "Caption";
            this.captionLabel.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // LineCurvesChart
            // 
            this.BackColor = System.Drawing.Color.White;
            this.Controls.Add(this.tableLayoutPanel1);
            this.Font = new System.Drawing.Font("Verdana", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Name = "LineCurvesChart";
            this.Size = new System.Drawing.Size(463, 355);
            this.Load += new System.EventHandler(this.LineCurvesChartType_Load);
            ((System.ComponentModel.ISupportInitialize)(this.chart1)).EndInit();
            this.panel1.ResumeLayout(false);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private void UpdateChartSettings() {
            // Set series chart type
            //chart1.Series["Series1"].ChartType = (SeriesChartType) Enum.Parse( typeof(SeriesChartType), comboBoxChartType.Text, true );
            //chart1.Series["Series2"].ChartType = (SeriesChartType) Enum.Parse( typeof(SeriesChartType), comboBoxChartType.Text, true );

            //// Set point labels
            //if(comboBoxPointLabels.Text != "None")
            //{
            //    chart1.Series["Series1"].IsValueShownAsLabel = true;
            //    chart1.Series["Series2"].IsValueShownAsLabel = true;
            //    if(comboBoxPointLabels.Text != "Auto")
            //    {
            //        chart1.Series["Series1"]["LabelStyle"] = comboBoxPointLabels.Text;
            //        chart1.Series["Series2"]["LabelStyle"] = comboBoxPointLabels.Text;
            //    }
            //}
            //else
            //{
            chart1.Series["Series1"].IsValueShownAsLabel = false;
            chart1.Series["Series2"].IsValueShownAsLabel = false;
            //}

            // Set X axis margin
            chart1.ChartAreas["Default"].AxisX.IsMarginVisible = checkBoxShowMargin.Checked;
        }

        private void LineCurvesChartType_Load(object sender, System.EventArgs e) {
            //comboBoxChartType.SelectedIndex = 0;
            //comboBoxPointLabels.SelectedIndex = 0;
            checkBoxShow3D.Checked = false;

            // Populate series data
            Random random = new Random();
            for (int pointIndex = 0; pointIndex < 10; pointIndex++) {
                chart1.Series["Series1"].Points.AddY(random.Next(45, 95));
                chart1.Series["Series2"].Points.AddY(random.Next(5, 65));
            }

            UpdateChartSettings();
        }

        private void comboBoxChartType_SelectedIndexChanged(object sender, System.EventArgs e) {
            UpdateChartSettings();
        }

        private void checkBoxShowMargin_CheckedChanged(object sender, System.EventArgs e) {
            UpdateChartSettings();
        }

        private void checkBoxShow3D_CheckedChanged(object sender, System.EventArgs e) {
            chart1.ChartAreas[0].Area3DStyle.Enable3D = checkBoxShow3D.Checked;
            if (checkBoxShow3D.Checked) {
                chart1.Series["Series1"].MarkerStyle = MarkerStyle.None;
                chart1.Series["Series2"].MarkerStyle = MarkerStyle.None;
                chart1.Series["Series1"].BorderWidth = 1;
                chart1.Series["Series2"].BorderWidth = 1;
            }
            else {
                chart1.Series["Series1"].MarkerStyle = MarkerStyle.Circle;
                chart1.Series["Series2"].MarkerStyle = MarkerStyle.Diamond;
                chart1.Series["Series1"].BorderWidth = 3;
                chart1.Series["Series2"].BorderWidth = 3;
            }
        }

        public string Caption {
            get { return captionLabel.Text; }
            set { captionLabel.Text = value; }
        }
    }
}