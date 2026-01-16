using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using System.Reflection;
using Ionic.Zip;
using System.Net;

namespace sqlBackup
{
    class BackupDb
    {
        private string hostname;
        private string username;
        private string password;
        private string[] dbname;
        private int numsOfDatabases;
        private string ftpHost;
        private string ftpUsername;
        private string ftpPassword;
        private String date = null;
        private String time = null;
        // mysqldump.exe path
        private string mysqldump = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName+"\\mysqldump";
        // user application path for sqlbackup(where everything will be saved here)
        private string user_path;

        public BackupDb()
        {

        }
        public BackupDb(string hostname,string username,string password, string[] dbname, string user_path, string FTPhost, string FTPusername, string FTPpassword)
        {
            this.hostname = hostname;
            this.password = password;
            this.username = username;
            this.dbname = dbname;
            this.user_path = user_path;
            this.numsOfDatabases = dbname.Length;
            this.ftpHost = FTPhost;
            this.ftpUsername = FTPusername;
            this.ftpPassword = FTPpassword;
        }

        // backup a database
        public string downloadDb()
        {
            if (numsOfDatabases > 0)
            {
                try
                {
                    System.
                    // DateTime object to take the date that backup has been complete
                    DateTime backupTime = DateTime.Now;

                    int year = backupTime.Year;
                    int month = backupTime.Month;

                    int day = backupTime.Day;
                    int hour = backupTime.Hour;

                    int minute = backupTime.Minute;
                    int second = backupTime.Second;

                    String tmestr = backupTime.ToString();
                    date = Convert.ToString(day)+"/"+ Convert.ToString(month) + "/" + Convert.ToString(year) ;
                    time = Convert.ToString(hour) + ":" + Convert.ToString(minute) + ":" + Convert.ToString(second);
                    string backup_name = "sqlBackup_" + day + "-" + month + "-" + year + "-" + hour + "_" + minute;
                    // name of subfolder for this backup
                    string backup_subfolder = user_path + "\\" + backup_name + "\\";

                    // check if subfolder directory exists, if not create it
                    System.IO.FileInfo create_subfolder = new System.IO.FileInfo(backup_subfolder);
                    create_subfolder.Directory.Create();

                    //get each selected database backup 
                    for (int i = 0; i < numsOfDatabases; i++)
                    {
                        // directory of backup(database) file
                        tmestr = backup_subfolder + dbname[i] + ".sql";

                        StreamWriter file = new StreamWriter(tmestr);
                        // start a new proc
                        ProcessStartInfo proc = new ProcessStartInfo();

                        // arguments for the proc, arguments for backup the database
                        string cmd = string.Format(@"-h{0} -u{1} -p{2} --opt --databases {3}", hostname, username, password, dbname[i]);

                        // set the mysqldump path as the proc filename, need for the success complete of below arguments  
                        proc.FileName = mysqldump;

                        proc.RedirectStandardInput = false;
                        proc.RedirectStandardOutput = true;

                        proc.Arguments = cmd;

                        proc.UseShellExecute = false;
                        Process p = Process.Start(proc);

                        string res;
                        // read the output of proc
                        res = p.StandardOutput.ReadToEnd();

                        // write the output in file 
                        file.WriteLine(res);
                        p.WaitForExit();
                        file.Close();
                    }

                    // zip the backup subfolder 
                    using (ZipFile zip = new ZipFile())
                    {
                        zip.UseUnicodeAsNecessary = true;
                        zip.AddDirectory(backup_subfolder);
                        zip.CompressionLevel = Ionic.Zlib.CompressionLevel.BestCompression;
                        zip.Comment = "This zip was created at " + System.DateTime.Now.ToString("G");
                        zip.Save(user_path+ "\\"+backup_name+".zip");
                    }
                    // after zip was created delete the backup subfolder
                    Directory.Delete(backup_subfolder, true);

                    uploadToFTP(user_path + "\\" + backup_name + ".zip", backup_name + ".zip");

                    return "Backup completed successfully!";

                }
                catch (IOException e)
                {
                    return "Error: "+e.Message.ToString();
                }
            }
            else return "Please first select database(s) for backup";
        }

        // upload backup file to FTP
        private void uploadToFTP(string sourceFile, string targetFile)
        {
            string filename = "ftp://" + ftpHost + "/" + targetFile;
            FtpWebRequest ftpReq = (FtpWebRequest)WebRequest.Create(filename);
            ftpReq.UseBinary = true;
            ftpReq.Method = WebRequestMethods.Ftp.UploadFile;
            ftpReq.Credentials = new NetworkCredential(ftpUsername, ftpPassword);

            byte[] b = File.ReadAllBytes(sourceFile);

            ftpReq.ContentLength = b.Length;
            using (Stream s = ftpReq.GetRequestStream())
            {
                s.Write(b, 0, b.Length);
            }
            
            FtpWebResponse ftpResp = (FtpWebResponse)ftpReq.GetResponse();
            
        }

        public string getPath()
        {
            return mysqldump;
        }
        public string getDate
        {
            get
            {
                return date;
            }
        }
        public string getTime
        {
            get
            {
                return time;
            }
        }
        public string[] getDbBackedUp
        {
            get { return dbname; }
        }
    }
    
}