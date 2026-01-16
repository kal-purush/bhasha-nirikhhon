namespace PokemonGo.RocketAPI.GUI
{
    partial class PokemonForm
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
            this.pokemonListView = new System.Windows.Forms.ListView();
            this.SuspendLayout();
            // 
            // pokemonListView
            // 
            this.pokemonListView.Location = new System.Drawing.Point(12, 12);
            this.pokemonListView.Name = "pokemonListView";
            this.pokemonListView.Size = new System.Drawing.Size(517, 280);
            this.pokemonListView.TabIndex = 0;
            this.pokemonListView.UseCompatibleStateImageBehavior = false;
            // 
            // PokemonForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(541, 302);
            this.Controls.Add(this.pokemonListView);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "PokemonForm";
            this.Text = "Pokemon List";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.ListView pokemonListView;
    }
}