using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Runtime.InteropServices;
using System.IO;
using Newtonsoft.Json;

namespace MultiScreenWallpaper
{
    public partial class frmWallpaperManagement : Form
    {
        [DllImport("user32.dll")]
        private static extern bool SystemParametersInfo(uint uiAction, uint uiParam, string pvParam, uint fWinIni);
        const uint SPI_SETDESKWALLPAPER = 0x14;
        const uint SPIF_UPDATEINIFILE = 0x01;
        string appPath = Path.GetDirectoryName(Application.ExecutablePath);

        public void SetDWallpaper(string path)
        {
            SystemParametersInfo(SPI_SETDESKWALLPAPER, 0, path, SPIF_UPDATEINIFILE);
        }

        public frmWallpaperManagement()
        {
            
            InitializeComponent();
            Microsoft.Win32.SystemEvents.DisplaySettingsChanged += new EventHandler(ScreenHandler);
        }

        private void ScreenHandler(object sender, EventArgs e)
        {
            loadWallpaper();
        }

        private void wallpaperTotalSize(ref int wallpaperTotalWidth, ref int wallpaperTotalHeight)
        {
            foreach (var screen in Screen.AllScreens)
            {
                wallpaperTotalWidth = wallpaperTotalWidth + screen.Bounds.Width;

                if (screen.Bounds.Height > wallpaperTotalHeight)
                {
                    wallpaperTotalHeight = screen.Bounds.Height;
                }
            }
        }

        static int GCD(int a, int b)
        {
            int Remainder;

            while (b != 0)
            {
                Remainder = a % b;
                a = b;
                b = Remainder;
            }

            return a;
        }

        private string aspectRatio(int x, int y)
        {
            return string.Format("{0}:{1}", x / GCD(x, y), y / GCD(x, y));
        }

        private void loadWallpaper()
        {
            int i;                          //Declare variable used for an index
            int wallpaperTotalWidth = 0;    //Declare variable used for total wallpaper width
            int wallpaperTotalHeight = 0;   //Declare variable used for total wallpaper height

            //Get total wallpaper size
            wallpaperTotalSize(ref wallpaperTotalWidth, ref wallpaperTotalHeight);

            //Declare list used for storing config
            List<string>[] config = new List<string>[2];

            string sJson = "";

            if (File.Exists(Application.StartupPath + @"\config.json"))
            {
                //Declare variable used to open config
                StreamReader streamReaderJson;

                //Read config
                streamReaderJson = new StreamReader(Application.StartupPath + @"\config.json");

                //Stroe contents of config.json in variable
                sJson = streamReaderJson.ReadToEnd();

                //Attempt to parse string
                try
                {

                    //Parse json config
                    config = JsonConvert.DeserializeObject<List<string>[]>(sJson);
                }

                //If parsing fails
                catch
                {

                    //Parse blank config
                    config = JsonConvert.DeserializeObject<List<string>[]>("[]");
                }

                var screenNames = new List<string>();       //Declare variable for storing screen names from config
                var wallpaperFiles = new List<string>();    //Declare variable for storing wallpaper file names from config
                screenNames = config[0];                    //Store wallpaper files from config
                wallpaperFiles = config[1];                 //Store screen names from config
                var imgwallpapers = new List<Image>();      //Declare variable for storing the wallpaper images

                //Reset index counter
                i = 0;

                //Loop for every wallpaper file
                foreach (var nul in wallpaperFiles)
                {

                    //Store wallpaper image to variable
                    imgwallpapers.Add(Image.FromFile(wallpaperFiles[i]));

                    //Add one to index
                    i++;
                }

                //Create wallpaper template
                Image imgWallpaper = new Bitmap(wallpaperTotalWidth, wallpaperTotalHeight);

                //Convert wallpaper template to graphic
                Graphics gWallpaper = Graphics.FromImage(imgWallpaper);

                //Declare variable used for tracking width used
                int screenWidthUsed = 0;

                //Loop for ever screen name
                foreach (var screenName in screenNames)
                {

                    bool wallpaperFound = false;

                    //Loop for every screen
                    foreach (var screen in Screen.AllScreens)
                    {

                        //If the name of the screen is equal to the X screen from config
                        if (screenName == screen.DeviceName)
                        {

                            foreach (var wallpaper in imgwallpapers)
                            {
                                if(aspectRatio(screen.Bounds.Width, screen.Bounds.Height) == aspectRatio(wallpaper.Width, wallpaper.Height))
                                {
                                    //Add wallpaper image to template
                                    gWallpaper.DrawImage(wallpaper, new Rectangle(screenWidthUsed, 0, screen.Bounds.Width, screen.Bounds.Height));

                                    wallpaperFound = true;
                                }
                            }

                            if (wallpaperFound == false)
                            {

                                //Add wallpaper image to template
                                gWallpaper.DrawImage(new Bitmap(1, 1), new Rectangle(screenWidthUsed, 0, screen.Bounds.Width, screen.Bounds.Height));
                            }

                            //Add screen width to the screen width used
                            screenWidthUsed = screenWidthUsed + screen.Bounds.Width;
                        }
                    }
                }

                //Save wallpaper
                imgWallpaper.Save("wallpaper.jpg");

                //Set wallpaper
                SetDWallpaper(appPath + "/wallpaper.jpg");
            }

            else
            {
                MessageBox.Show("Wallpaper Management: Config Not Found");
                this.Close();
            }
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            Opacity = 0;
            loadWallpaper();
        }

        private void Form_Shown(object sender, EventArgs e)
        {
            Visible = false;
            Opacity = 100;
        }
    }
}