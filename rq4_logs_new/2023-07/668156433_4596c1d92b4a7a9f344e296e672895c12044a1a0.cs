namespace PingPongGame
{
    partial class GameForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(GameForm));
            this.cpuPlayer = new System.Windows.Forms.PictureBox();
            this.userPlayer = new System.Windows.Forms.PictureBox();
            this.ball = new System.Windows.Forms.PictureBox();
            this.score = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.cpuPlayer)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.userPlayer)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ball)).BeginInit();
            this.SuspendLayout();
            // 
            // cpuPlayer
            // 
            this.cpuPlayer.BackColor = System.Drawing.Color.Transparent;
            this.cpuPlayer.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("cpuPlayer.BackgroundImage")));
            this.cpuPlayer.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.cpuPlayer.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.cpuPlayer.Location = new System.Drawing.Point(12, 133);
            this.cpuPlayer.Name = "cpuPlayer";
            this.cpuPlayer.Size = new System.Drawing.Size(21, 135);
            this.cpuPlayer.TabIndex = 0;
            this.cpuPlayer.TabStop = false;
            // 
            // userPlayer
            // 
            this.userPlayer.BackColor = System.Drawing.Color.Transparent;
            this.userPlayer.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("userPlayer.BackgroundImage")));
            this.userPlayer.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.userPlayer.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.userPlayer.Location = new System.Drawing.Point(887, 133);
            this.userPlayer.Name = "userPlayer";
            this.userPlayer.Size = new System.Drawing.Size(21, 135);
            this.userPlayer.TabIndex = 1;
            this.userPlayer.TabStop = false;
            // 
            // ball
            // 
            this.ball.BackColor = System.Drawing.Color.Transparent;
            this.ball.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("ball.BackgroundImage")));
            this.ball.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ball.Location = new System.Drawing.Point(338, 200);
            this.ball.Name = "ball";
            this.ball.Size = new System.Drawing.Size(48, 48);
            this.ball.TabIndex = 2;
            this.ball.TabStop = false;
            // 
            // score
            // 
            this.score.AutoSize = true;
            this.score.BackColor = System.Drawing.Color.Transparent;
            this.score.Font = new System.Drawing.Font("Consolas", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.score.ForeColor = System.Drawing.SystemColors.Control;
            this.score.Location = new System.Drawing.Point(865, 9);
            this.score.Name = "score";
            this.score.Size = new System.Drawing.Size(43, 23);
            this.score.TabIndex = 3;
            this.score.Text = "000";
            this.score.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.BackColor = System.Drawing.Color.Transparent;
            this.label1.Font = new System.Drawing.Font("Consolas", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.SystemColors.ActiveCaption;
            this.label1.Location = new System.Drawing.Point(794, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(65, 23);
            this.label1.TabIndex = 4;
            this.label1.Text = "Score";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // GameForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(920, 501);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.score);
            this.Controls.Add(this.ball);
            this.Controls.Add(this.userPlayer);
            this.Controls.Add(this.cpuPlayer);
            this.Name = "GameForm";
            this.Text = "SSS";
            ((System.ComponentModel.ISupportInitialize)(this.cpuPlayer)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.userPlayer)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ball)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox cpuPlayer;
        private System.Windows.Forms.PictureBox userPlayer;
        private System.Windows.Forms.PictureBox ball;
        private System.Windows.Forms.Label score;
        private System.Windows.Forms.Label label1;
    }
}
