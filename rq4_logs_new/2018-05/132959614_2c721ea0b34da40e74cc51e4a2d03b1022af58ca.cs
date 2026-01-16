using System;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Windows.Forms;

namespace RouletteGameSimple
{
    public partial class FrmMain : Form
    {

        private int timeCycle = 0;
        private Timer tmrRoulette = null;
        private Stopwatch stopwatch = new Stopwatch();

        private float angle = 0;
        private Image imgRoulette = null;

        private string[] valuesDiscounts = { "5%", "10%", "15%", "20%", "25%", "30%" };
        private int[] posxsDiscounts = { 235, 400, 400, 235, 65, 65 };
        private int[] posysDiscounts = { 18, 118, 308, 408, 308, 118 };

        public FrmMain()
        {
            InitializeComponent();
        }

        private void FrmJuego_Load(object sender, EventArgs e)
        {
            try
            {

                this.lblArrow.BackColor = Color.White;
                this.lblArrow.Text = "";

                this.imgRoulette = (Image)Properties.Resources._06_Ruleta_circulo;

                var rouletteBase = (Image)this.imgRoulette.Clone();
                var rouletteConDescuentos = SetValues(rouletteBase);
                var rouletteConLogo = SetLogo(rouletteConDescuentos);
                this.picRoulette.Image = rouletteConLogo;

                this.stopwatch = new Stopwatch(); 

                this.tmrRoulette = new Timer();
                this.tmrRoulette.Enabled = false;
                this.tmrRoulette.Interval = 50;
                this.tmrRoulette.Tick += new EventHandler(tmrroulette_Tick);

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, this.Text);
            }
        }

        private void btnJugar_Click(object sender, EventArgs e)
        {
            try
            {
                if (this.btnPlay.Enabled)
                {
                    this.lblArrow.BackColor = Color.White;
                    this.lblArrow.Text = "";

                    this.btnPlay.Enabled = false;
                    this.tmrRoulette.Enabled = true;

                    this.timeCycle = new Random().Next(3000, 5000);

                    this.stopwatch.Start();
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, this.Text);
            }
        }

        private void tmrroulette_Tick(object sender, EventArgs e)
        {
            try
            {
                angle += 30.0f;

                var bitmap = (Bitmap)this.imgRoulette;
                var imgrouletteGirada = (Image)RotateImage(bitmap, angle);
                var imgrouletteFinal = SetLogo(imgrouletteGirada);

                if (angle >= 360)
                    angle = 0.0f;

                if (this.stopwatch.ElapsedMilliseconds >= this.timeCycle)
                {
                    if (angle % 60 > 0)
                    {
                        angle = ((int)(angle / 60) + 1) * 60;
                        if (angle >= 360)
                            angle = 0.0f;

                        bitmap = (Bitmap)this.imgRoulette;
                        imgrouletteGirada = (Image)RotateImage(bitmap, angle);
                        imgrouletteFinal = SetLogo(imgrouletteGirada);
                    }

                    this.stopwatch.Reset();

                    this.btnPlay.Enabled = true;
                    this.tmrRoulette.Enabled = false;

                    imgrouletteFinal = SetValues(imgrouletteFinal);

                    int selected = (int)this.angle / 60;
                    this.lblArrow.Text = valuesDiscounts[selected];
                    //this.lblFlecha.BackColor = Color.White;
                }

                this.picRoulette.Image = imgrouletteFinal;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, this.Text);
            }
        }

        private Image SetLogo(Image roulette)
        {
            Image rouletteWithLogo = null;
            try
            {

                var rouletteBase = (Image)roulette.Clone();

                var rouletteLogo = (Image)Properties.Resources._06_Ruleta_logo_centro;
                int posxLogo = (int)((rouletteBase.Width / 2) - (rouletteLogo.Width / 2));
                int posyLogo = (int)((rouletteBase.Height / 2) - (rouletteLogo.Height / 2));

                using (Graphics graphics = Graphics.FromImage(rouletteBase))
                {
                    graphics.DrawImage(rouletteLogo, new Point(posxLogo, posyLogo));
                }

                rouletteWithLogo = rouletteBase;

                return rouletteWithLogo;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private Image SetValues(Image roulette)
        {
            Image rouletteConDescuentos = null;
            try
            {

                var rouletteBase = (Image)roulette.Clone();

                var drawFormat = new StringFormat();
                drawFormat.Alignment = StringAlignment.Center;

                var arialFont = new Font("Arial Black", 20);

                using (Graphics graphics = Graphics.FromImage(rouletteBase))
                {
                    int posicion = (int)this.angle / 60;
                    for (int i = 0; i < 6; i++)
                    {

                        int posX = posxsDiscounts[i];
                        int posY = posysDiscounts[i];

                        string valor = valuesDiscounts[posicion];

                        graphics.DrawString(valor, arialFont, Brushes.Black, posX, posY, drawFormat);

                        posicion++;
                        if (posicion >= 6)
                            posicion = 0;
                    }
                }

                rouletteConDescuentos = rouletteBase;

                return rouletteConDescuentos;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private Bitmap RotateImage(Bitmap b, float angle)
        {
            //Create a new empty bitmap to hold rotated image.
            Bitmap returnBitmap = new Bitmap(b.Width, b.Height);

            //Make a graphics object from the empty bitmap.
            using (Graphics g = Graphics.FromImage(returnBitmap))
            {
                //move rotation point to center of image.
                g.InterpolationMode = InterpolationMode.HighQualityBicubic;
                g.TranslateTransform((float)b.Width / 2, (float)b.Height / 2);
                //Rotate.        
                g.RotateTransform(angle);
                //Move image back.
                g.TranslateTransform(-(float)b.Width / 2, -(float)b.Height / 2);

                //Draw passed in image onto graphics object.
                g.DrawImage(b, new Point(0, 0));
                //g.DrawImage(b, 0, 0, b.Width, b.Height);
            }

            return returnBitmap;
        }

        public Point RotatePoint(float angle, Point pt)
        {
            try
            {
                double a = angle * Math.PI / 180.0;
                double cosa = Math.Cos(a);
                double sina = Math.Sin(a);
                int pointX = (int)(pt.X * cosa - pt.Y * sina);
                int pointY = (int)(pt.X * sina + pt.Y * cosa);
                return new Point(pointX, pointY);
            }
            catch (Exception)
            {

                throw;
            }
        }
        
    }
}