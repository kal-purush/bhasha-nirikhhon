using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QRCoder;
using System.Drawing;
using System.IO;
using System.Diagnostics;
using System.Management;

namespace QRCoder1
{
    class Program
    {
        static void Main(string[] args)
        {
            String installedProducts ="";
            ManagementObjectSearcher mos = new ManagementObjectSearcher("SELECT * FROM Win32_Product");
            foreach (ManagementObject mo in mos.Get())
            {
                String installDateString = (String)mo["InstallDate"];
                DateTime installDate = DateTime.ParseExact(installDateString, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

                //SAMPLE DATE TIME (TODO REMOVE / CHANGE) 
                DateTime todayDate = new DateTime(2016, 3, 1, 0, 0, 0);

                //Compare date time against a time TODO CHANGE TO MIDNIGHT TODAY 
                if (DateTime.Compare(installDate, todayDate) > 0){ //if greater than 0, t1 is later than t2    
                    String productName = (String)mo["Name"];
                    String version = (String)mo["Version"];
                    installedProducts = installedProducts + productName + "," + version + "\r\n";
                }
            }

            //create QR Code as bitmap
            QRCodeGenerator qrGenerator = new QRCodeGenerator();
            QRCodeData qrCodeData = qrGenerator.CreateQrCode(installedProducts, QRCodeGenerator.ECCLevel.Q);
            QRCode qrCode = new QRCode(qrCodeData);
            Bitmap qrCodeImage = qrCode.GetGraphic(20);

            //create a path based on the current running user 
            String username = Environment.UserName;
            String path = String.Format("C:\\Users\\{0}\\Desktop\\qrcode.bmp", username);
            //save bitmap
            qrCodeImage.Save(path);
            //open bitmap
            Process.Start(@path);
            //Console.ReadLine();
            

        }
    }
}