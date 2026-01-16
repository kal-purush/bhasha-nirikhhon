namespace FilmsRecommendations
{
    partial class Form2
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
            this.labelTypeAnswer = new System.Windows.Forms.Label();
            this.listBoxAnswerType = new System.Windows.Forms.ListBox();
            this.label1 = new System.Windows.Forms.Label();
            this.labelActor = new System.Windows.Forms.Label();
            this.labelFilm = new System.Windows.Forms.Label();
            this.labelJanre = new System.Windows.Forms.Label();
            this.labelCountry = new System.Windows.Forms.Label();
            this.textBoxActor = new System.Windows.Forms.TextBox();
            this.textBoxJanre = new System.Windows.Forms.TextBox();
            this.textBoxFilm = new System.Windows.Forms.TextBox();
            this.textBoxCountry = new System.Windows.Forms.TextBox();
            this.buttonGetAnswer = new System.Windows.Forms.Button();
            this.buttonAddParametrs = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.textBoxAnwerAsString = new System.Windows.Forms.TextBox();
            this.buttonClear = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // labelTypeAnswer
            // 
            this.labelTypeAnswer.AutoSize = true;
            this.labelTypeAnswer.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.labelTypeAnswer.Location = new System.Drawing.Point(73, 36);
            this.labelTypeAnswer.Name = "labelTypeAnswer";
            this.labelTypeAnswer.Size = new System.Drawing.Size(166, 18);
            this.labelTypeAnswer.TabIndex = 0;
            this.labelTypeAnswer.Text = "Выберите тип вопроса";
            // 
            // listBoxAnswerType
            // 
            this.listBoxAnswerType.FormattingEnabled = true;
            this.listBoxAnswerType.Items.AddRange(new object[] {
            "Играл ли актер в фильме",
            "Имеет ли актер оскар",
            "Имеет ли фильм этот жанр",
            "Снимался ли фильм в этой стране",
            "Хорош ли фильм",
            "Плох ли фильм",
            "Превосходный ли фильм",
            "Обычный ли фильм"});
            this.listBoxAnswerType.Location = new System.Drawing.Point(66, 68);
            this.listBoxAnswerType.Name = "listBoxAnswerType";
            this.listBoxAnswerType.Size = new System.Drawing.Size(199, 95);
            this.listBoxAnswerType.TabIndex = 1;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.label1.Location = new System.Drawing.Point(379, 36);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(203, 18);
            this.label1.TabIndex = 2;
            this.label1.Text = "Введите требуемые данные";
            // 
            // labelActor
            // 
            this.labelActor.AutoSize = true;
            this.labelActor.Location = new System.Drawing.Point(356, 67);
            this.labelActor.Name = "labelActor";
            this.labelActor.Size = new System.Drawing.Size(67, 13);
            this.labelActor.TabIndex = 3;
            this.labelActor.Text = "Имя актера";
            // 
            // labelFilm
            // 
            this.labelFilm.AutoSize = true;
            this.labelFilm.Location = new System.Drawing.Point(338, 94);
            this.labelFilm.Name = "labelFilm";
            this.labelFilm.Size = new System.Drawing.Size(100, 13);
            this.labelFilm.TabIndex = 4;
            this.labelFilm.Text = "Название фильма";
            // 
            // labelJanre
            // 
            this.labelJanre.AutoSize = true;
            this.labelJanre.Location = new System.Drawing.Point(379, 120);
            this.labelJanre.Name = "labelJanre";
            this.labelJanre.Size = new System.Drawing.Size(36, 13);
            this.labelJanre.TabIndex = 5;
            this.labelJanre.Text = "Жанр";
            // 
            // labelCountry
            // 
            this.labelCountry.AutoSize = true;
            this.labelCountry.Location = new System.Drawing.Point(380, 150);
            this.labelCountry.Name = "labelCountry";
            this.labelCountry.Size = new System.Drawing.Size(43, 13);
            this.labelCountry.TabIndex = 6;
            this.labelCountry.Text = "Страна";
            // 
            // textBoxActor
            // 
            this.textBoxActor.Location = new System.Drawing.Point(464, 64);
            this.textBoxActor.Name = "textBoxActor";
            this.textBoxActor.Size = new System.Drawing.Size(100, 20);
            this.textBoxActor.TabIndex = 8;
            // 
            // textBoxJanre
            // 
            this.textBoxJanre.Location = new System.Drawing.Point(464, 117);
            this.textBoxJanre.Name = "textBoxJanre";
            this.textBoxJanre.Size = new System.Drawing.Size(100, 20);
            this.textBoxJanre.TabIndex = 9;
            // 
            // textBoxFilm
            // 
            this.textBoxFilm.Location = new System.Drawing.Point(464, 91);
            this.textBoxFilm.Name = "textBoxFilm";
            this.textBoxFilm.Size = new System.Drawing.Size(100, 20);
            this.textBoxFilm.TabIndex = 10;
            // 
            // textBoxCountry
            // 
            this.textBoxCountry.Location = new System.Drawing.Point(464, 147);
            this.textBoxCountry.Name = "textBoxCountry";
            this.textBoxCountry.Size = new System.Drawing.Size(100, 20);
            this.textBoxCountry.TabIndex = 11;
            // 
            // buttonGetAnswer
            // 
            this.buttonGetAnswer.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.buttonGetAnswer.Location = new System.Drawing.Point(394, 259);
            this.buttonGetAnswer.Name = "buttonGetAnswer";
            this.buttonGetAnswer.Size = new System.Drawing.Size(100, 57);
            this.buttonGetAnswer.TabIndex = 12;
            this.buttonGetAnswer.Text = "Получить ответ";
            this.buttonGetAnswer.UseVisualStyleBackColor = true;
            this.buttonGetAnswer.Click += new System.EventHandler(this.buttonGetAnswer_Click);
            // 
            // buttonAddParametrs
            // 
            this.buttonAddParametrs.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.buttonAddParametrs.Location = new System.Drawing.Point(283, 259);
            this.buttonAddParametrs.Name = "buttonAddParametrs";
            this.buttonAddParametrs.Size = new System.Drawing.Size(105, 57);
            this.buttonAddParametrs.TabIndex = 13;
            this.buttonAddParametrs.Text = "Добавить еще параметры";
            this.buttonAddParametrs.UseVisualStyleBackColor = true;
            this.buttonAddParametrs.Click += new System.EventHandler(this.buttonAddParametrs_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(170, 196);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(310, 13);
            this.label2.TabIndex = 14;
            this.label2.Text = "Или введите свой вопрос в форме логического выражения";
            // 
            // textBoxAnwerAsString
            // 
            this.textBoxAnwerAsString.Location = new System.Drawing.Point(66, 212);
            this.textBoxAnwerAsString.Name = "textBoxAnwerAsString";
            this.textBoxAnwerAsString.Size = new System.Drawing.Size(498, 20);
            this.textBoxAnwerAsString.TabIndex = 15;
            this.textBoxAnwerAsString.Text = "Vx.(HasActor(x, ACTOR_EDWARD_NORTON)->IsAwesome(x))";
            // 
            // buttonClear
            // 
            this.buttonClear.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.buttonClear.Location = new System.Drawing.Point(172, 259);
            this.buttonClear.Name = "buttonClear";
            this.buttonClear.Size = new System.Drawing.Size(105, 57);
            this.buttonClear.TabIndex = 16;
            this.buttonClear.Text = "Сбросить";
            this.buttonClear.UseVisualStyleBackColor = true;
            this.buttonClear.Click += new System.EventHandler(this.buttonClear_Click);
            // 
            // Form2
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(630, 328);
            this.Controls.Add(this.buttonClear);
            this.Controls.Add(this.textBoxAnwerAsString);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.buttonAddParametrs);
            this.Controls.Add(this.buttonGetAnswer);
            this.Controls.Add(this.textBoxCountry);
            this.Controls.Add(this.textBoxFilm);
            this.Controls.Add(this.textBoxJanre);
            this.Controls.Add(this.textBoxActor);
            this.Controls.Add(this.labelCountry);
            this.Controls.Add(this.labelJanre);
            this.Controls.Add(this.labelFilm);
            this.Controls.Add(this.labelActor);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.listBoxAnswerType);
            this.Controls.Add(this.labelTypeAnswer);
            this.Name = "Form2";
            this.Text = "Обратный поиск";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label labelTypeAnswer;
        private System.Windows.Forms.ListBox listBoxAnswerType;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label labelActor;
        private System.Windows.Forms.Label labelFilm;
        private System.Windows.Forms.Label labelJanre;
        private System.Windows.Forms.Label labelCountry;
        private System.Windows.Forms.TextBox textBoxActor;
        private System.Windows.Forms.TextBox textBoxJanre;
        private System.Windows.Forms.TextBox textBoxFilm;
        private System.Windows.Forms.TextBox textBoxCountry;
        private System.Windows.Forms.Button buttonGetAnswer;
        private System.Windows.Forms.Button buttonAddParametrs;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox textBoxAnwerAsString;
        private System.Windows.Forms.Button buttonClear;
    }
}