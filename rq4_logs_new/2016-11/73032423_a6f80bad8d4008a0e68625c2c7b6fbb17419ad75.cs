using System;
using System.Collections.Generic;
 
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace FreeTextBox1.CQEdit
{
	public partial class ftbimagegallery : System.Web.UI.Page
	{
        // Messages
        private string NoFileMessage = "您没有选择文件。";
      
       // private string NoImagesMessage = "该文件夹不存在或者是空的";
       
        private string NoFileToDeleteMessage = "您没有选中要删除的文件。";
       // private string InvalidFileTypeMessage = "您无法上传这种类型的文件。";

        private string[] AcceptedFileTypes = new string[] { "jpg", "jpeg", "jpe", "gif", "png" };

        // Configuration
     /*   private bool UploadIsEnabled = true;         // 是否允许上传文件
        private bool DeleteIsEnabled = true;         // 是否允许删除文件
        private string DefaultImageFolder = "images";  // 默认的起始文件夹*/


    
        /// <summary>
        /// 上传文件 非公共的文件目录
        /// </summary>
        private string ImageGalleryPath = "";

        /// <summary>
        /// 公共目录 的当前目录
        /// </summary>
        private string PublicImageGalleryPath = "";

        /// <summary>
        /// 公共根目录
        /// </summary>
        private string PublicImageRootFolder = "";

        protected void Page_Load(object sender, EventArgs e)
        {
            string isframe = "" + Request["frame"];

            //上传文件目录 非公开的
            if (Request["rif"] != null)
            {
                ImageGalleryPath = Request["rif"].ToString();
                PublicImageGalleryPath = ImageGalleryPath; //默认 公开的当前目录为 上传文件的目录
                PublicImageRootFolder = ImageGalleryPath; //保存公开图片的根目录 默认 上传文件的目录
            }

            //公共图片根目录 如果有 参数的话 
            if (Request["PublicImageGalleryPath"] != null)
            {
                PublicImageGalleryPath = Request["PublicImageGalleryPath"].ToString();
                if (PublicImageGalleryPath.StartsWith("/"))
                {
                    PublicImageGalleryPath = PublicImageGalleryPath.Remove(0, 1);                   
                }
                PublicImageRootFolder = PublicImageGalleryPath;
            }

            if (Request["UpPath"] != null) //如果有下级目录
            {
                PublicImageGalleryPath = Request["UpPath"].ToString();
            }
            if (isframe != "")
            {
                MainPage.Visible = true;
                iframePanel.Visible = false;
              
                CurrentImagesFolder.Value = PublicImageGalleryPath;
                RootImagesFolder.Value = PublicImageRootFolder;

                if (!IsPostBack)
                {                   
                    DisplayImages();
                }

            }

            UploadImage.Attributes.Add("onclick", "javascript:return CheckFilename(this.value);");
        }

        public void UploadImage_OnClick(object sender, EventArgs e)
        {

            HttpPostedFile postFile = UploadFile.PostedFile;
            if (postFile.ContentLength < 10)
            {
                ResultsMessage.Text = NoFileMessage;
                return;
            }
            string fileName, fullName;

            fileName = postFile.FileName;
            GetFileName(fileName, out fullName);
            postFile.SaveAs(Server.MapPath(fullName));
           
            if (!CkbSC.Checked) //不是公共的
            {
                ClientScriptManager _page = this.ClientScript;
                if (!CkbSC.Checked) //不是公共的
                {
                    System.Drawing.Image myImage = System.Drawing.Image.FromFile(Server.MapPath(fullName));
                    _page.RegisterStartupScript(this.GetType(), "返回图片", "<script>returnImage('" + fullName + "','" + myImage.Width + "','" + myImage.Height + "');</script>", false);
                }

            }
            DisplayImages();
        }

        /// <summary>
        /// 返回文件名称 
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fullName"></param>
        void GetFileName(string fileName, out string fullName)
        {
            int lastDot = fileName.LastIndexOf(".");
            string fileExtension = fileName.Substring(lastDot, fileName.Length - lastDot);
            string filePath = "";
            string AppUrl = "";
            //Get the application's URL
            if (Request.ApplicationPath == "/")
                AppUrl = Request.ApplicationPath;
            else
                AppUrl = Request.ApplicationPath + "/";

            if (!CkbSC.Checked) //不是公共的
            {
                filePath = ImageGalleryPath+"/";
            }
            else
            {
                filePath = CurrentImagesFolder.Value + "/";
            }

            filePath = AppUrl + filePath;



            string Download = System.Web.HttpContext.Current.Server.MapPath(filePath);
            if (!System.IO.Directory.Exists(Download))
            {
                System.IO.Directory.CreateDirectory(Download);
            }

            DateTime current = DateTime.Now;
            int year, month, day, hour, minute, second, millsecond;
            year = current.Year;
            month = current.Month;
            day = current.Day;
            hour = current.Hour;
            minute = current.Millisecond;
            second = current.Second;
            millsecond = current.Millisecond;
            fullName = filePath + year.ToString() + month.ToString() + day.ToString() + hour.ToString() + minute.ToString() + second.ToString() + millsecond.ToString() + fileExtension.ToLower();
        }



        public void DeleteImage_OnClick(object sender, EventArgs e)
        {
            if (FileToDelete.Value != "" && FileToDelete.Value != "undefined")
            {
                try
                {
                    string AppPath = HttpContext.Current.Request.PhysicalApplicationPath;
                    System.IO.File.Delete(AppPath + CurrentImagesFolder.Value + "\\" + FileToDelete.Value);
                    ResultsMessage.Text = "已删除: " + FileToDelete.Value;
                }
                catch (Exception ex)
                {
                    ResultsMessage.Text = "删除失败。";
                    throw ex;
                }
            }
            else
            {
                ResultsMessage.Text = NoFileToDeleteMessage;
            }
            DisplayImages();
        }

        private bool IsValidFileType(string FileName)
        {
            string ext = FileName.Substring(FileName.LastIndexOf(".") + 1, FileName.Length - FileName.LastIndexOf(".") - 1);
            for (int i = 0; i < AcceptedFileTypes.Length; i++)
            {
                if (ext == AcceptedFileTypes[i])
                {
                    return true;

                }
            }
            return false;
        }


        /// <summary>
        /// 返回文件夹下的 文件
        /// </summary>
        /// <returns></returns>
        private string[] ReturnFilesArray()
        {

            try
            {
                string AppPath = HttpContext.Current.Request.PhysicalApplicationPath;
                string ImageFolderPath = AppPath + CurrentImagesFolder.Value;
                string[] FilesArray = System.IO.Directory.GetFiles(ImageFolderPath, "*");
                return FilesArray;
            }
            catch
            {

                return null;
            }


        }

        /// <summary>
        /// 返回文件夹下的 文件夹
        /// </summary>
        /// <returns></returns>
        private string[] ReturnDirectoriesArray()
        {
            try
            {
                string AppPath = HttpContext.Current.Request.PhysicalApplicationPath;
                string CurrentFolderPath = AppPath + CurrentImagesFolder.Value;
                string[] DirectoriesArray = System.IO.Directory.GetDirectories(CurrentFolderPath, "*");
                return DirectoriesArray;
            }
            catch
            {
                return null;
            }
        }




        public void DisplayImages()
        {
            string[] FilesArray = ReturnFilesArray();
            string[] DirectoriesArray = ReturnDirectoriesArray();
            string AppPath = HttpContext.Current.Request.PhysicalApplicationPath;
            string AppUrl;

            //Get the application's URL
            if (Request.ApplicationPath == "/")
                AppUrl = Request.ApplicationPath;
            else
                AppUrl = Request.ApplicationPath + "/";

                  GalleryPanel.Controls.Clear();
           
                string ImageFileName = "";
                string ImageFileLocation = "";

                int thumbWidth = 94;
                int thumbHeight = 94;

                string ParentFolder = "";
                if (CurrentImagesFolder.Value != RootImagesFolder.Value)
                {
                    #region 上级目录
                    System.Web.UI.HtmlControls.HtmlImage myHtmlImage = new System.Web.UI.HtmlControls.HtmlImage();
                    myHtmlImage.Src = AppUrl + "images/ftb/folder.up.gif";
                    myHtmlImage.Attributes["unselectable"] = "on";
                    myHtmlImage.Attributes["align"] = "absmiddle";
                    myHtmlImage.Attributes["vspace"] = "36";

                     ParentFolder = CurrentImagesFolder.Value.Substring(0, CurrentImagesFolder.Value.LastIndexOf("/"));

                    System.Web.UI.WebControls.Panel myImageHolder = new System.Web.UI.WebControls.Panel();
                    myImageHolder.CssClass = "imageholder";
                    myImageHolder.Attributes["unselectable"] = "on";
                    myImageHolder.Attributes["onclick"] = "divClick(this,'p','','19','22');"; // 图片目录 公共目录 新的目录

                    myImageHolder.Attributes["ondblclick"] = "gotoFolder('" + ImageGalleryPath + "','" + RootImagesFolder.Value + "','" + ParentFolder.Replace("\\", "\\\\") + "');";
                    myImageHolder.Controls.Add(myHtmlImage);

                    System.Web.UI.WebControls.Panel myMainHolder = new System.Web.UI.WebControls.Panel();
                    myMainHolder.CssClass = "imagespacer";
                    myMainHolder.Controls.Add(myImageHolder);

                    System.Web.UI.WebControls.Panel myTitleHolder = new System.Web.UI.WebControls.Panel();
                    myTitleHolder.CssClass = "titleHolder";
                    myTitleHolder.Controls.Add(new LiteralControl("向上"));
                    myMainHolder.Controls.Add(myTitleHolder);

                    GalleryPanel.Controls.Add(myMainHolder);
                    #endregion 

                }
                if (DirectoriesArray == null)
                {
                    gallerymessage.Text = "&nbsp;&nbsp;&nbsp;&nbsp;图片位置 公共图片库:\\   0个文件夹  0个文件";
                    return;
                }
                foreach (string _Directory in DirectoriesArray)
                {
                    #region 文件夹
                    try
                    {
                        string DirectoryName = _Directory.ToString();


                        System.Web.UI.HtmlControls.HtmlImage myHtmlImage = new System.Web.UI.HtmlControls.HtmlImage();
                        myHtmlImage.Src = AppUrl + "images/ftb/folder.big.gif";
                        myHtmlImage.Attributes["unselectable"] = "on";
                        myHtmlImage.Attributes["align"] = "absmiddle";
                        myHtmlImage.Attributes["vspace"] = "29";

                        System.Web.UI.WebControls.Panel myImageHolder = new System.Web.UI.WebControls.Panel();
                        myImageHolder.CssClass = "imageholder";
                        myImageHolder.Attributes["unselectable"] = "on";
                        myImageHolder.Attributes["onclick"] = "divClick(this,'d','" + DirectoryName.Replace(AppPath + CurrentImagesFolder.Value + "\\", "") + "','38','36');";
                        myImageHolder.Attributes["ondblclick"] = "gotoFolder('" + ImageGalleryPath + "','" + RootImagesFolder.Value + "','" + DirectoryName.Replace(AppPath, "").Replace("\\", "/") + "');";
                        myImageHolder.Controls.Add(myHtmlImage);

                        System.Web.UI.WebControls.Panel myMainHolder = new System.Web.UI.WebControls.Panel();
                        myMainHolder.CssClass = "imagespacer";
                        myMainHolder.Controls.Add(myImageHolder);

                        System.Web.UI.WebControls.Panel myTitleHolder = new System.Web.UI.WebControls.Panel();
                        myTitleHolder.CssClass = "titleHolder";
                        myTitleHolder.Controls.Add(new LiteralControl(DirectoryName.Replace(AppPath + CurrentImagesFolder.Value + "\\", "")));
                        myMainHolder.Controls.Add(myTitleHolder);

                        GalleryPanel.Controls.Add(myMainHolder);
                    }
                    catch
                    {
                        // nothing for error
                    }
                    #endregion 
                }

                int Num = 0;
                foreach (string ImageFile in FilesArray)
                {
                    #region 文件
                    try
                    {

                        ImageFileName = ImageFile.ToString();
                        ImageFileName = ImageFileName.Substring(ImageFileName.LastIndexOf("\\") + 1);
                        ImageFileLocation = AppUrl;
                        ImageFileLocation = ImageFileLocation.Substring(ImageFileLocation.LastIndexOf("\\") + 1);
                        //galleryfilelocation += "/";
                        ImageFileLocation += CurrentImagesFolder.Value;
                        ImageFileLocation += "/";
                        ImageFileLocation += ImageFileName;
                        string imgURL = ImageFileLocation;
                        System.Web.UI.HtmlControls.HtmlImage myHtmlImage = new System.Web.UI.HtmlControls.HtmlImage();
                        myHtmlImage.Src = ImageFileLocation;

                        System.Drawing.Image myImage = System.Drawing.Image.FromFile(ImageFile.ToString());
                        myHtmlImage.Attributes["unselectable"] = "on";
                        //myHtmlImage.border=0;

                        // landscape image
                        if (myImage.Width > myImage.Height)
                        {
                            if (myImage.Width > thumbWidth)
                            {
                                myHtmlImage.Width = thumbWidth;
                                myHtmlImage.Height = Convert.ToInt32(myImage.Height * thumbWidth / myImage.Width);
                            }
                            else
                            {
                                myHtmlImage.Width = myImage.Width;
                                myHtmlImage.Height = myImage.Height;
                            }
                            // portrait image
                        }
                        else
                        {
                            if (myImage.Height > thumbHeight)
                            {
                                myHtmlImage.Height = thumbHeight;
                                myHtmlImage.Width = Convert.ToInt32(myImage.Width * thumbHeight / myImage.Height);
                            }
                            else
                            {
                                myHtmlImage.Width = myImage.Width;
                                myHtmlImage.Height = myImage.Height;
                            }
                        }

                        if (myHtmlImage.Height < thumbHeight)
                        {
                            myHtmlImage.Attributes["vspace"] = Convert.ToInt32((thumbHeight / 2) - (myHtmlImage.Height / 2)).ToString();
                        }

                        int lastDot = ImageFileName.LastIndexOf(".");
                        string fileExtension = ImageFileName.Substring(lastDot, ImageFileName.Length - lastDot);

                        string TmpImageFileName = ++Num + fileExtension.ToLower();
                      
                        System.Web.UI.WebControls.Panel myImageHolder = new System.Web.UI.WebControls.Panel();
                        myImageHolder.CssClass = "imageholder";
                        myImageHolder.Attributes["onclick"] = "divClick(this,'" + ImageFileName + "','" + imgURL + "','" + myHtmlImage.Width + "','" + myHtmlImage.Height + "','" + (Num).ToString() + fileExtension.ToLower()+"');";
                        myImageHolder.Attributes["ondblclick"] = "returnImage('" + ImageFileLocation.Replace("\\", "/") + "','" + myImage.Width.ToString() + "','" + myImage.Height.ToString() + "');";
                        myImageHolder.Controls.Add(myHtmlImage);


                        System.Web.UI.WebControls.Panel myMainHolder = new System.Web.UI.WebControls.Panel();
                        myMainHolder.CssClass = "imagespacer";
                        myMainHolder.Controls.Add(myImageHolder);

                        System.Web.UI.WebControls.Panel myTitleHolder = new System.Web.UI.WebControls.Panel();
                        myTitleHolder.CssClass = "titleHolder";
                      
                        myTitleHolder.Controls.Add(new LiteralControl(TmpImageFileName + "<BR>" + myImage.Width.ToString() + "x" + myImage.Height.ToString()));
                        myMainHolder.Controls.Add(myTitleHolder);

                        GalleryPanel.Controls.Add(myMainHolder);

                        myImage.Dispose();
                    }
                    catch
                    {

                    }
                    #endregion 
                }
                string NowPath = CurrentImagesFolder.Value.Replace(RootImagesFolder.Value,"");
                if (NowPath == "")
                    NowPath = "公共图库:\\";
                else
                    NowPath = "公共图库:"+NowPath.Replace("/","\\");
                gallerymessage.Text = "&nbsp;&nbsp;&nbsp;&nbsp;图片位置 " + NowPath + "  " + DirectoriesArray.Length + "个文件夹  " + FilesArray.Length + "个文件";
            }
       
	}
}