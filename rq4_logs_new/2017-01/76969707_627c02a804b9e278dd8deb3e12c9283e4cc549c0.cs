            this.tabVatandas.Size = new System.Drawing.Size(793, 457);
            this.panel1.Size = new System.Drawing.Size(465, 422);
            this.listViewPeople.Size = new System.Drawing.Size(367, 412);
            this.tabKulllanici.Size = new System.Drawing.Size(793, 438);
            this.tabControlKulEkle.Size = new System.Drawing.Size(801, 483);
            this.ClientSize = new System.Drawing.Size(825, 543);
            this.MinimumSize = new System.Drawing.Size(841, 582);
            this.listView.Size = new System.Drawing.Size(626, 218);
            this.ClientSize = new System.Drawing.Size(650, 314);
﻿namespace WindowsProje
{
    partial class Vatandaş
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
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Text = "Vatandaş";
        }

        #endregion
    }
}
﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsProje
{
    public partial class Vatandaş : Form
    {
        public Vatandaş(ListViewItem item)
        {
            InitializeComponent();
        }
    }
}
﻿namespace WindowsProje
{
    partial class VatandaşForm
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
            this.SuspendLayout();
            // 
            // VatandaşForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(725, 429);
            this.IsMdiContainer = true;
            this.Name = "VatandaşForm";
            this.Text = "VatandaşForm";
            this.ResumeLayout(false);

        }

        #endregion
    }
}
﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsProje
{
    public partial class VatandaşForm : Form
    {
        private static VatandaşForm form = null;

        private VatandaşForm()
        {
            InitializeComponent();
        }

        public static VatandaşForm getForm()
        {
            if (form == null)
                form = new VatandaşForm();
            return form;
        }

        public void openWindow(Vatandaş vatandaş)
        {

        }
    }
}