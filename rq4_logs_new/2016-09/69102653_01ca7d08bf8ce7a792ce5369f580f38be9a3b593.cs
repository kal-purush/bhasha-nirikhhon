partial class Form1
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
         this.btnFileOpen = new System.Windows.Forms.Button();
         this.label2 = new System.Windows.Forms.Label();
         this.numAlpha = new System.Windows.Forms.NumericUpDown();
         this.btnDeleteSnake = new System.Windows.Forms.Button();
         this.grpUpdate = new System.Windows.Forms.GroupBox();
         this.label3 = new System.Windows.Forms.Label();
         this.numIntervalMS = new System.Windows.Forms.NumericUpDown();
         this.chkShowForces = new System.Windows.Forms.CheckBox();
         this.radUpdateAuto = new System.Windows.Forms.RadioButton();
         this.radUpdateManual = new System.Windows.Forms.RadioButton();
         this.updateTimer = new System.Windows.Forms.Timer(this.components);
         this.pnlControls = new System.Windows.Forms.Panel();
         this.btnHalfPoints = new System.Windows.Forms.Button();
         this.btnDoublePoints = new System.Windows.Forms.Button();
         this.grpSnakeForces = new System.Windows.Forms.GroupBox();
         this.label1 = new System.Windows.Forms.Label();
         this.numDelta = new System.Windows.Forms.NumericUpDown();
         this.lblCurve = new System.Windows.Forms.Label();
         this.numBeta = new System.Windows.Forms.NumericUpDown();
         this.grpImageForce = new System.Windows.Forms.GroupBox();
         this.numGamma = new System.Windows.Forms.NumericUpDown();
         this.label4 = new System.Windows.Forms.Label();
         this.radForceEdge = new System.Windows.Forms.RadioButton();
         this.radForceSmooth = new System.Windows.Forms.RadioButton();
         this.radForceGray = new System.Windows.Forms.RadioButton();
         this.grpDisplay = new System.Windows.Forms.GroupBox();
         this.numGauss = new System.Windows.Forms.NumericUpDown();
         this.radEdges = new System.Windows.Forms.RadioButton();
         this.radSmoothed = new System.Windows.Forms.RadioButton();
         this.radGrayscale = new System.Windows.Forms.RadioButton();
         this.radOriginal = new System.Windows.Forms.RadioButton();
         ((System.ComponentModel.ISupportInitialize)(this.numAlpha)).BeginInit();
         this.grpUpdate.SuspendLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numIntervalMS)).BeginInit();
         this.pnlControls.SuspendLayout();
         this.grpSnakeForces.SuspendLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numDelta)).BeginInit();
         ((System.ComponentModel.ISupportInitialize)(this.numBeta)).BeginInit();
         this.grpImageForce.SuspendLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numGamma)).BeginInit();
         this.grpDisplay.SuspendLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numGauss)).BeginInit();
         this.SuspendLayout();
         // 
         // btnFileOpen
         // 
         this.btnFileOpen.Location = new System.Drawing.Point(11, 11);
         this.btnFileOpen.Name = "btnFileOpen";
         this.btnFileOpen.Size = new System.Drawing.Size(120, 23);
         this.btnFileOpen.TabIndex = 2;
         this.btnFileOpen.Text = "Load Image";
         this.btnFileOpen.UseVisualStyleBackColor = true;
         this.btnFileOpen.Click += new System.EventHandler(this.btnFileOpen_Click);
         // 
         // label2
         // 
         this.label2.AutoSize = true;
         this.label2.Location = new System.Drawing.Point(6, 19);
         this.label2.Name = "label2";
         this.label2.Size = new System.Drawing.Size(70, 13);
         this.label2.TabIndex = 4;
         this.label2.Text = "alpha (elast.):";
         this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
         // 
         // numAlpha
         // 
         this.numAlpha.DecimalPlaces = 2;
         this.numAlpha.Increment = new decimal(new int[] {
            1,
            0,
            0,
            131072});
         this.numAlpha.Location = new System.Drawing.Point(79, 17);
         this.numAlpha.Maximum = new decimal(new int[] {
            2,
            0,
            0,
            0});
         this.numAlpha.Minimum = new decimal(new int[] {
            2,
            0,
            0,
            -2147483648});
         this.numAlpha.Name = "numAlpha";
         this.numAlpha.Size = new System.Drawing.Size(54, 20);
         this.numAlpha.TabIndex = 5;
         this.numAlpha.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
         this.numAlpha.ValueChanged += new System.EventHandler(this.numAlpha_ValueChanged);
         // 
         // btnDeleteSnake
         // 
         this.btnDeleteSnake.Enabled = false;
         this.btnDeleteSnake.Location = new System.Drawing.Point(11, 40);
         this.btnDeleteSnake.Name = "btnDeleteSnake";
         this.btnDeleteSnake.Size = new System.Drawing.Size(120, 23);
         this.btnDeleteSnake.TabIndex = 15;
         this.btnDeleteSnake.Text = "Delete Snake";
         this.btnDeleteSnake.UseVisualStyleBackColor = true;
         this.btnDeleteSnake.Click += new System.EventHandler(this.btnDeleteSnake_Click);
         // 
         // grpUpdate
         // 
         this.grpUpdate.Controls.Add(this.label3);
         this.grpUpdate.Controls.Add(this.numIntervalMS);
         this.grpUpdate.Controls.Add(this.chkShowForces);
         this.grpUpdate.Controls.Add(this.radUpdateAuto);
         this.grpUpdate.Controls.Add(this.radUpdateManual);
         this.grpUpdate.Enabled = false;
         this.grpUpdate.Location = new System.Drawing.Point(609, 11);
         this.grpUpdate.Name = "grpUpdate";
         this.grpUpdate.Size = new System.Drawing.Size(144, 100);
         this.grpUpdate.TabIndex = 20;
         this.grpUpdate.TabStop = false;
         this.grpUpdate.Text = "Update Mode";
         // 
         // label3
         // 
         this.label3.AutoSize = true;
         this.label3.Location = new System.Drawing.Point(8, 46);
         this.label3.Name = "label3";
         this.label3.Size = new System.Drawing.Size(67, 13);
         this.label3.TabIndex = 25;
         this.label3.Text = "Interval (ms):";
         this.label3.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
         // 
         // numIntervalMS
         // 
         this.numIntervalMS.Increment = new decimal(new int[] {
            5,
            0,
            0,
            0});
         this.numIntervalMS.Location = new System.Drawing.Point(81, 44);
         this.numIntervalMS.Maximum = new decimal(new int[] {
            200,
            0,
            0,
            0});
         this.numIntervalMS.Minimum = new decimal(new int[] {
            5,
            0,
            0,
            0});
         this.numIntervalMS.Name = "numIntervalMS";
         this.numIntervalMS.Size = new System.Drawing.Size(50, 20);
         this.numIntervalMS.TabIndex = 24;
         this.numIntervalMS.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
         this.numIntervalMS.Value = new decimal(new int[] {
            100,
            0,
            0,
            0});
         this.numIntervalMS.ValueChanged += new System.EventHandler(this.numIntervalMS_ValueChanged_1);
         // 
         // chkShowForces
         // 
         this.chkShowForces.AutoSize = true;
         this.chkShowForces.Checked = true;
         this.chkShowForces.CheckState = System.Windows.Forms.CheckState.Checked;
         this.chkShowForces.Location = new System.Drawing.Point(11, 73);
         this.chkShowForces.Name = "chkShowForces";
         this.chkShowForces.Size = new System.Drawing.Size(122, 17);
         this.chkShowForces.TabIndex = 23;
         this.chkShowForces.Text = "Show Force Vectors";
         this.chkShowForces.UseVisualStyleBackColor = true;
         this.chkShowForces.CheckedChanged += new System.EventHandler(this.chkShowForces_CheckedChanged);
         // 
         // radUpdateAuto
         // 
         this.radUpdateAuto.AutoSize = true;
         this.radUpdateAuto.Location = new System.Drawing.Point(80, 16);
         this.radUpdateAuto.Name = "radUpdateAuto";
         this.radUpdateAuto.Size = new System.Drawing.Size(51, 17);
         this.radUpdateAuto.TabIndex = 21;
         this.radUpdateAuto.Text = "Timer";
         this.radUpdateAuto.UseVisualStyleBackColor = true;
         this.radUpdateAuto.CheckedChanged += new System.EventHandler(this.radUpdateAutoSlow_CheckedChanged);
         // 
         // radUpdateManual
         // 
         this.radUpdateManual.AutoSize = true;
         this.radUpdateManual.Checked = true;
         this.radUpdateManual.Location = new System.Drawing.Point(9, 16);
         this.radUpdateManual.Name = "radUpdateManual";
         this.radUpdateManual.Size = new System.Drawing.Size(47, 17);
         this.radUpdateManual.TabIndex = 20;
         this.radUpdateManual.TabStop = true;
         this.radUpdateManual.Text = "Stop";
         this.radUpdateManual.UseVisualStyleBackColor = true;
         this.radUpdateManual.CheckedChanged += new System.EventHandler(this.radUpdateManual_CheckedChanged);
         // 
         // updateTimer
         // 
         this.updateTimer.Tick += new System.EventHandler(this.updateTimer_Tick);
         // 
         // pnlControls
         // 
         this.pnlControls.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
         this.pnlControls.Controls.Add(this.btnHalfPoints);
         this.pnlControls.Controls.Add(this.btnDoublePoints);
         this.pnlControls.Controls.Add(this.grpSnakeForces);
         this.pnlControls.Controls.Add(this.grpImageForce);
         this.pnlControls.Controls.Add(this.btnFileOpen);
         this.pnlControls.Controls.Add(this.grpDisplay);
         this.pnlControls.Controls.Add(this.grpUpdate);
         this.pnlControls.Controls.Add(this.btnDeleteSnake);
         this.pnlControls.Dock = System.Windows.Forms.DockStyle.Bottom;
         this.pnlControls.Location = new System.Drawing.Point(0, 440);
         this.pnlControls.Name = "pnlControls";
         this.pnlControls.Size = new System.Drawing.Size(764, 122);
         this.pnlControls.TabIndex = 22;
         // 
         // btnHalfPoints
         // 
         this.btnHalfPoints.Enabled = false;
         this.btnHalfPoints.Location = new System.Drawing.Point(11, 64);
         this.btnHalfPoints.Name = "btnHalfPoints";
         this.btnHalfPoints.Size = new System.Drawing.Size(120, 23);
         this.btnHalfPoints.TabIndex = 25;
         this.btnHalfPoints.Text = "Half Points";
         this.btnHalfPoints.UseVisualStyleBackColor = true;
         this.btnHalfPoints.Click += new System.EventHandler(this.btnHalfPoints_Click);
         // 
         // btnDoublePoints
         // 
         this.btnDoublePoints.Enabled = false;
         this.btnDoublePoints.Location = new System.Drawing.Point(11, 88);
         this.btnDoublePoints.Name = "btnDoublePoints";
         this.btnDoublePoints.Size = new System.Drawing.Size(120, 23);
         this.btnDoublePoints.TabIndex = 24;
         this.btnDoublePoints.Text = "Double Points";
         this.btnDoublePoints.UseVisualStyleBackColor = true;
         this.btnDoublePoints.Click += new System.EventHandler(this.btnDoublePoints_Click);
         // 
         // grpSnakeForces
         // 
         this.grpSnakeForces.Controls.Add(this.label1);
         this.grpSnakeForces.Controls.Add(this.numDelta);
         this.grpSnakeForces.Controls.Add(this.lblCurve);
         this.grpSnakeForces.Controls.Add(this.numBeta);
         this.grpSnakeForces.Controls.Add(this.numAlpha);
         this.grpSnakeForces.Controls.Add(this.label2);
         this.grpSnakeForces.Enabled = false;
         this.grpSnakeForces.Location = new System.Drawing.Point(141, 11);
         this.grpSnakeForces.Name = "grpSnakeForces";
         this.grpSnakeForces.Size = new System.Drawing.Size(140, 100);
         this.grpSnakeForces.TabIndex = 23;
         this.grpSnakeForces.TabStop = false;
         this.grpSnakeForces.Text = "Inner Snake Forces";
         // 
         // label1
         // 
         this.label1.AutoSize = true;
         this.label1.Location = new System.Drawing.Point(6, 65);
         this.label1.Name = "label1";
         this.label1.Size = new System.Drawing.Size(38, 13);
         this.label1.TabIndex = 9;
         this.label1.Text = "ballon:";
         this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
         // 
         // numDelta
         // 
         this.numDelta.DecimalPlaces = 2;
         this.numDelta.Increment = new decimal(new int[] {
            1,
            0,
            0,
            131072});
         this.numDelta.Location = new System.Drawing.Point(79, 63);
         this.numDelta.Maximum = new decimal(new int[] {
            2,
            0,
            0,
            0});
         this.numDelta.Minimum = new decimal(new int[] {
            2,
            0,
            0,
            -2147483648});
         this.numDelta.Name = "numDelta";
         this.numDelta.Size = new System.Drawing.Size(54, 20);
         this.numDelta.TabIndex = 8;
         this.numDelta.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
         this.numDelta.ValueChanged += new System.EventHandler(this.numDelta_ValueChanged);
         // 
         // lblCurve
         // 
         this.lblCurve.AutoSize = true;
         this.lblCurve.Location = new System.Drawing.Point(6, 40);
         this.lblCurve.Name = "lblCurve";
         this.lblCurve.Size = new System.Drawing.Size(67, 13);
         this.lblCurve.TabIndex = 7;
         this.lblCurve.Text = "beta (curve):";
         this.lblCurve.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
         // 
         // numBeta
         // 
         this.numBeta.DecimalPlaces = 2;
         this.numBeta.Increment = new decimal(new int[] {
            1,
            0,
            0,
            131072});
         this.numBeta.Location = new System.Drawing.Point(79, 40);
         this.numBeta.Maximum = new decimal(new int[] {
            2,
            0,
            0,
            0});
         this.numBeta.Minimum = new decimal(new int[] {
            2,
            0,
            0,
            -2147483648});
         this.numBeta.Name = "numBeta";
         this.numBeta.Size = new System.Drawing.Size(54, 20);
         this.numBeta.TabIndex = 6;
         this.numBeta.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
         this.numBeta.ValueChanged += new System.EventHandler(this.numBeta_ValueChanged);
         // 
         // grpImageForce
         // 
         this.grpImageForce.Controls.Add(this.numGamma);
         this.grpImageForce.Controls.Add(this.label4);
         this.grpImageForce.Controls.Add(this.radForceEdge);
         this.grpImageForce.Controls.Add(this.radForceSmooth);
         this.grpImageForce.Controls.Add(this.radForceGray);
         this.grpImageForce.Enabled = false;
         this.grpImageForce.Location = new System.Drawing.Point(287, 11);
         this.grpImageForce.Name = "grpImageForce";
         this.grpImageForce.Size = new System.Drawing.Size(135, 100);
         this.grpImageForce.TabIndex = 22;
         this.grpImageForce.TabStop = false;
         this.grpImageForce.Text = "Outer Image Force";
         // 
         // numGamma
         // 
         this.numGamma.DecimalPlaces = 2;
         this.numGamma.Increment = new decimal(new int[] {
            1,
            0,
            0,
            131072});
         this.numGamma.Location = new System.Drawing.Point(75, 17);
         this.numGamma.Maximum = new decimal(new int[] {
            5,
            0,
            0,
            0});
         this.numGamma.Minimum = new decimal(new int[] {
            5,
            0,
            0,
            -2147483648});
         this.numGamma.Name = "numGamma";
         this.numGamma.Size = new System.Drawing.Size(54, 20);
         this.numGamma.TabIndex = 23;
         this.numGamma.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
         this.numGamma.ValueChanged += new System.EventHandler(this.numGamma_ValueChanged);
         // 
         // label4
         // 
         this.label4.Location = new System.Drawing.Point(7, 19);
         this.label4.Name = "label4";
         this.label4.Size = new System.Drawing.Size(66, 13);
         this.label4.TabIndex = 24;
         this.label4.Text = "gamma:";
         this.label4.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
         // 
         // radForceEdge
         // 
         this.radForceEdge.AutoSize = true;
         this.radForceEdge.Checked = true;
         this.radForceEdge.Location = new System.Drawing.Point(7, 76);
         this.radForceEdge.Name = "radForceEdge";
         this.radForceEdge.Size = new System.Drawing.Size(106, 17);
         this.radForceEdge.TabIndex = 20;
         this.radForceEdge.TabStop = true;
         this.radForceEdge.Text = "Smoothed Edges";
         this.radForceEdge.UseVisualStyleBackColor = true;
         this.radForceEdge.CheckedChanged += new System.EventHandler(this.radForceEdge_CheckedChanged);
         // 
         // radForceSmooth
         // 
         this.radForceSmooth.AutoSize = true;
         this.radForceSmooth.Location = new System.Drawing.Point(7, 57);
         this.radForceSmooth.Name = "radForceSmooth";
         this.radForceSmooth.Size = new System.Drawing.Size(123, 17);
         this.radForceSmooth.TabIndex = 19;
         this.radForceSmooth.Text = "Smoothed Grayscale";
         this.radForceSmooth.UseVisualStyleBackColor = true;
         this.radForceSmooth.CheckedChanged += new System.EventHandler(this.radForceSmooth_CheckedChanged);
         // 
         // radForceGray
         // 
         this.radForceGray.AutoSize = true;
         this.radForceGray.Location = new System.Drawing.Point(7, 38);
         this.radForceGray.Name = "radForceGray";
         this.radForceGray.Size = new System.Drawing.Size(72, 17);
         this.radForceGray.TabIndex = 18;
         this.radForceGray.Text = "Grayscale";
         this.radForceGray.UseVisualStyleBackColor = true;
         this.radForceGray.CheckedChanged += new System.EventHandler(this.radForceGray_CheckedChanged);
         // 
         // grpDisplay
         // 
         this.grpDisplay.Controls.Add(this.numGauss);
         this.grpDisplay.Controls.Add(this.radEdges);
         this.grpDisplay.Controls.Add(this.radSmoothed);
         this.grpDisplay.Controls.Add(this.radGrayscale);
         this.grpDisplay.Controls.Add(this.radOriginal);
         this.grpDisplay.Enabled = false;
         this.grpDisplay.Location = new System.Drawing.Point(428, 11);
         this.grpDisplay.Name = "grpDisplay";
         this.grpDisplay.Size = new System.Drawing.Size(175, 100);
         this.grpDisplay.TabIndex = 21;
         this.grpDisplay.TabStop = false;
         this.grpDisplay.Text = "Display";
         // 
         // numGauss
         // 
         this.numGauss.Increment = new decimal(new int[] {
            2,
            0,
            0,
            0});
         this.numGauss.Location = new System.Drawing.Point(129, 55);
         this.numGauss.Maximum = new decimal(new int[] {
            21,
            0,
            0,
            0});
         this.numGauss.Minimum = new decimal(new int[] {
            3,
            0,
            0,
            0});
         this.numGauss.Name = "numGauss";
         this.numGauss.Size = new System.Drawing.Size(39, 20);
         this.numGauss.TabIndex = 21;
         this.numGauss.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
         this.numGauss.Value = new decimal(new int[] {
            13,
            0,
            0,
            0});
         this.numGauss.ValueChanged += new System.EventHandler(this.numGauss_ValueChanged);
         // 
         // radEdges
         // 
         this.radEdges.AutoSize = true;
         this.radEdges.Location = new System.Drawing.Point(9, 74);
         this.radEdges.Name = "radEdges";
         this.radEdges.Size = new System.Drawing.Size(106, 17);
         this.radEdges.TabIndex = 20;
         this.radEdges.Text = "Smoothed Edges";
         this.radEdges.UseVisualStyleBackColor = true;
         this.radEdges.CheckedChanged += new System.EventHandler(this.radEdges_CheckedChanged);
         // 
         // radSmoothed
         // 
         this.radSmoothed.AutoSize = true;
         this.radSmoothed.Location = new System.Drawing.Point(9, 55);
         this.radSmoothed.Name = "radSmoothed";
         this.radSmoothed.Size = new System.Drawing.Size(122, 17);
         this.radSmoothed.TabIndex = 19;
         this.radSmoothed.Text = "Smoothed, Strength:";
         this.radSmoothed.UseVisualStyleBackColor = true;
         this.radSmoothed.CheckedChanged += new System.EventHandler(this.radSmoothed_CheckedChanged);
         // 
         // radGrayscale
         // 
         this.radGrayscale.AutoSize = true;
         this.radGrayscale.Location = new System.Drawing.Point(9, 36);
         this.radGrayscale.Name = "radGrayscale";
         this.radGrayscale.Size = new System.Drawing.Size(72, 17);
         this.radGrayscale.TabIndex = 18;
         this.radGrayscale.Text = "Grayscale";
         this.radGrayscale.UseVisualStyleBackColor = true;
         this.radGrayscale.CheckedChanged += new System.EventHandler(this.radGrayscale_CheckedChanged);
         // 
         // radOriginal
         // 
         this.radOriginal.AutoSize = true;
         this.radOriginal.Checked = true;
         this.radOriginal.Location = new System.Drawing.Point(9, 17);
         this.radOriginal.Name = "radOriginal";
         this.radOriginal.Size = new System.Drawing.Size(92, 17);
         this.radOriginal.TabIndex = 17;
         this.radOriginal.TabStop = true;
         this.radOriginal.Text = "Original Image";
         this.radOriginal.UseVisualStyleBackColor = true;
         this.radOriginal.CheckedChanged += new System.EventHandler(this.radOriginal_CheckedChanged);
         // 
         // Form1
         // 
         this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
         this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
         this.ClientSize = new System.Drawing.Size(764, 562);
         this.Controls.Add(this.pnlControls);
         this.MinimumSize = new System.Drawing.Size(780, 600);
         this.Name = "Form1";
         this.Text = "Snake Test";
         this.Load += new System.EventHandler(this.Form1_Load);
         this.Paint += new System.Windows.Forms.PaintEventHandler(this.Form1_Paint);
         this.MouseDown += new System.Windows.Forms.MouseEventHandler(this.Form1_MouseDown);
         ((System.ComponentModel.ISupportInitialize)(this.numAlpha)).EndInit();
         this.grpUpdate.ResumeLayout(false);
         this.grpUpdate.PerformLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numIntervalMS)).EndInit();
         this.pnlControls.ResumeLayout(false);
         this.grpSnakeForces.ResumeLayout(false);
         this.grpSnakeForces.PerformLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numDelta)).EndInit();
         ((System.ComponentModel.ISupportInitialize)(this.numBeta)).EndInit();
         this.grpImageForce.ResumeLayout(false);
         this.grpImageForce.PerformLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numGamma)).EndInit();
         this.grpDisplay.ResumeLayout(false);
         this.grpDisplay.PerformLayout();
         ((System.ComponentModel.ISupportInitialize)(this.numGauss)).EndInit();
         this.ResumeLayout(false);

   }

   #endregion

   public System.Windows.Forms.Button btnFileOpen;
   public System.Windows.Forms.Label label2;
   public System.Windows.Forms.NumericUpDown numAlpha;
   public System.Windows.Forms.Button btnDeleteSnake;
   public System.Windows.Forms.GroupBox grpUpdate;
   public System.Windows.Forms.RadioButton radUpdateAuto;
   public System.Windows.Forms.RadioButton radUpdateManual;
   public System.Windows.Forms.Timer updateTimer;
   public System.Windows.Forms.Panel pnlControls;
   public System.Windows.Forms.GroupBox grpImageForce;
   public System.Windows.Forms.RadioButton radForceEdge;
   public System.Windows.Forms.RadioButton radForceSmooth;
   public System.Windows.Forms.RadioButton radForceGray;
   public System.Windows.Forms.GroupBox grpDisplay;
   public System.Windows.Forms.RadioButton radEdges;
   public System.Windows.Forms.RadioButton radSmoothed;
   public System.Windows.Forms.RadioButton radGrayscale;
   public System.Windows.Forms.RadioButton radOriginal;
   public System.Windows.Forms.NumericUpDown numGamma;
   public System.Windows.Forms.Label label4;
   public System.Windows.Forms.GroupBox grpSnakeForces;
   public System.Windows.Forms.NumericUpDown numGauss;
   public System.Windows.Forms.Label lblCurve;
   public System.Windows.Forms.NumericUpDown numBeta;
   public System.Windows.Forms.Button btnDoublePoints;
   public System.Windows.Forms.Button btnHalfPoints;
   public System.Windows.Forms.CheckBox chkShowForces;
   public System.Windows.Forms.NumericUpDown numDelta;
   public System.Windows.Forms.Label label1;
   public System.Windows.Forms.Label label3;
   public System.Windows.Forms.NumericUpDown numIntervalMS;
}
