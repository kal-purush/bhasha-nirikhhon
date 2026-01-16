namespace DXSample
{
    partial class Main
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
            this.components = new System.ComponentModel.Container();
            this.gridControl1 = new DevExpress.XtraGrid.GridControl();
            this.advBandedGridView1 = new DevExpress.XtraGrid.Views.BandedGrid.AdvBandedGridView();
            this.recordBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.colID = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colParentID = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colText = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colInfo = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colValue = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colDt = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colState = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colImage = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.colDetail = new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn();
            this.gridBand1 = new DevExpress.XtraGrid.Views.BandedGrid.GridBand();
            this.gridBand2 = new DevExpress.XtraGrid.Views.BandedGrid.GridBand();
            ((System.ComponentModel.ISupportInitialize)(this.gridControl1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.advBandedGridView1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.recordBindingSource)).BeginInit();
            this.SuspendLayout();
            // 
            // gridControl1
            // 
            this.gridControl1.DataSource = this.recordBindingSource;
            this.gridControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.gridControl1.Location = new System.Drawing.Point(0, 0);
            this.gridControl1.MainView = this.advBandedGridView1;
            this.gridControl1.Name = "gridControl1";
            this.gridControl1.Size = new System.Drawing.Size(714, 459);
            this.gridControl1.TabIndex = 0;
            this.gridControl1.ViewCollection.AddRange(new DevExpress.XtraGrid.Views.Base.BaseView[] {
            this.advBandedGridView1});
            // 
            // advBandedGridView1
            // 
            this.advBandedGridView1.Bands.AddRange(new DevExpress.XtraGrid.Views.BandedGrid.GridBand[] {
            this.gridBand1,
            this.gridBand2});
            this.advBandedGridView1.Columns.AddRange(new DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn[] {
            this.colID,
            this.colParentID,
            this.colText,
            this.colInfo,
            this.colValue,
            this.colDt,
            this.colState,
            this.colImage,
            this.colDetail});
            this.advBandedGridView1.GridControl = this.gridControl1;
            this.advBandedGridView1.Name = "advBandedGridView1";
            this.advBandedGridView1.OptionsView.ColumnAutoWidth = true;
            this.advBandedGridView1.CustomDrawColumnHeader += new DevExpress.XtraGrid.Views.Grid.ColumnHeaderCustomDrawEventHandler(this.advBandedGridView1_CustomDrawColumnHeader);
            this.advBandedGridView1.FocusedColumnChanged += new DevExpress.XtraGrid.Views.Base.FocusedColumnChangedEventHandler(this.advBandedGridView1_FocusedColumnChanged);
            // 
            // recordBindingSource
            // 
            this.recordBindingSource.DataSource = typeof(DXSample.Record);
            // 
            // colID
            // 
            this.colID.FieldName = "ID";
            this.colID.Name = "colID";
            this.colID.Visible = true;
            // 
            // colParentID
            // 
            this.colParentID.FieldName = "ParentID";
            this.colParentID.Name = "colParentID";
            this.colParentID.RowIndex = 1;
            this.colParentID.Visible = true;
            this.colParentID.Width = 225;
            // 
            // colText
            // 
            this.colText.FieldName = "Text";
            this.colText.Name = "colText";
            this.colText.Visible = true;
            // 
            // colInfo
            // 
            this.colInfo.FieldName = "Info";
            this.colInfo.Name = "colInfo";
            this.colInfo.Visible = true;
            this.colInfo.Width = 213;
            // 
            // colValue
            // 
            this.colValue.FieldName = "Value";
            this.colValue.Name = "colValue";
            this.colValue.Visible = true;
            // 
            // colDt
            // 
            this.colDt.FieldName = "Dt";
            this.colDt.Name = "colDt";
            this.colDt.RowIndex = 2;
            this.colDt.Visible = true;
            this.colDt.Width = 225;
            // 
            // colState
            // 
            this.colState.FieldName = "State";
            this.colState.Name = "colState";
            this.colState.RowIndex = 1;
            this.colState.Visible = true;
            this.colState.Width = 213;
            // 
            // colImage
            // 
            this.colImage.FieldName = "Image";
            this.colImage.Name = "colImage";
            this.colImage.RowIndex = 2;
            this.colImage.Visible = true;
            this.colImage.Width = 213;
            // 
            // colDetail
            // 
            this.colDetail.FieldName = "Detail";
            this.colDetail.Name = "colDetail";
            // 
            // gridBand1
            // 
            this.gridBand1.Caption = "Key Information";
            this.gridBand1.Columns.Add(this.colID);
            this.gridBand1.Columns.Add(this.colText);
            this.gridBand1.Columns.Add(this.colValue);
            this.gridBand1.Columns.Add(this.colDetail);
            this.gridBand1.Columns.Add(this.colParentID);
            this.gridBand1.Columns.Add(this.colDt);
            this.gridBand1.Name = "gridBand1";
            this.gridBand1.VisibleIndex = 0;
            this.gridBand1.Width = 225;
            // 
            // gridBand2
            // 
            this.gridBand2.Caption = "Additional Information";
            this.gridBand2.Columns.Add(this.colInfo);
            this.gridBand2.Columns.Add(this.colState);
            this.gridBand2.Columns.Add(this.colImage);
            this.gridBand2.Name = "gridBand2";
            this.gridBand2.VisibleIndex = 1;
            this.gridBand2.Width = 213;
            // 
            // Main
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(714, 459);
            this.Controls.Add(this.gridControl1);
            this.Name = "Main";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.gridControl1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.advBandedGridView1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.recordBindingSource)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private DevExpress.XtraGrid.GridControl gridControl1;
        private DevExpress.XtraGrid.Views.BandedGrid.AdvBandedGridView advBandedGridView1;
        private System.Windows.Forms.BindingSource recordBindingSource;
        private DevExpress.XtraGrid.Views.BandedGrid.GridBand gridBand1;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colID;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colText;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colValue;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colDetail;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colParentID;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colDt;
        private DevExpress.XtraGrid.Views.BandedGrid.GridBand gridBand2;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colInfo;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colState;
        private DevExpress.XtraGrid.Views.BandedGrid.BandedGridColumn colImage;
    }
}
