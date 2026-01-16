using System;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows.Media.Imaging;
using Humanizer.Bytes;
using Microsoft.WindowsAPICodePack.Shell;


namespace Models
{
    public class Folder : Element
    {
        public ObservableCollection<Element> Children { get; set; } = new ObservableCollection<Element>();
        public string CountOfFolders { get; set; }
        public string CountOfFiles { get; set; }
        public string CreationTime { get; set; }
        public bool Selected { get; set; }
        bool _isExpanded;
        public bool IsExpanded
        {
            get { return _isExpanded; }
            set
            {
                if (value != _isExpanded)
                {
                    _isExpanded = value;
                    this.RaisePropertyChanged("IsExpanded");
                }

            }
        }


        internal void GetChildren()
        {
            Children.Clear();

            try
            {


                foreach (DirectoryInfo dir in new DirectoryInfo(FullName).GetDirectories().OrderBy(x => x.Name).ToArray())
                {
                    ShellObject shellFolder = ShellObject.FromParsingName(dir.FullName.ToString());
                    BitmapSource shellThumb = shellFolder.Thumbnail.SmallBitmapSource;

                    Folder newItem = new Folder
                    {
                        Name = dir.ToString(),
                        Icon = shellThumb,
                        FullName = dir.FullName,
                    };


                    try
                    {
                        newItem.CountOfFolders = new DirectoryInfo(dir.FullName).GetDirectories().Length.ToString();
                    }
                    catch (UnauthorizedAccessException ex)
                    {
                        newItem.CountOfFolders = "Отказано в доступе";
                    }

                    try
                    {
                        newItem.CountOfFiles = new DirectoryInfo(dir.FullName).GetFiles().Length.ToString();
                    }
                    catch (UnauthorizedAccessException ex)
                    {
                        newItem.CountOfFiles = "Отказано в доступе";
                    }

                    try
                    {
                        newItem.CreationTime = dir.CreationTime.ToString(CultureInfo.InvariantCulture);
                    }
                    catch (UnauthorizedAccessException ex)
                    {

                    }


                    newItem.Children.Add(new Element
                    {
                        Name = "*",
                        Icon = null,
                        FullName = dir.FullName
                    });

                    Children.Add(newItem);
                }

                foreach (FileInfo file in new DirectoryInfo(FullName).GetFiles().OrderBy(x => x.Name).ToArray())
                {

                    System.Drawing.Icon icon = (System.Drawing.Icon)System.Drawing.Icon.ExtractAssociatedIcon(file.FullName.ToString());


                    if (file.Extension.ToLower() == ".jpg" || file.Extension.ToLower() == ".jpeg" || file.Extension.ToLower() == ".bmp" || file.Extension.ToLower() == ".png")
                    {
                        Children.Add(new CustomImage
                        {
                            Name = file.ToString(),
                            Icon = icon.ToImageSource(),
                            FullName = file.FullName.ToString(),
                            Size = ByteSize.FromBytes(file.Length).ToString(),
                            CreationTime = file.CreationTime.ToString(CultureInfo.InvariantCulture)
                        });
                    }
                    else
                    {
                        Children.Add(new CustomFile
                        {
                            Name = file.ToString(),
                            Icon = icon.ToImageSource(),
                            FullName = file.FullName,
                            Size = ByteSize.FromBytes(file.Length).ToString(),
                            CreationTime = file.CreationTime.ToString(CultureInfo.InvariantCulture)
                        });
                    }



                }

            }
            catch (UnauthorizedAccessException ex)
            {
                Folder newIFolder = new Folder()
                {
                    Name = "Отказано в доступе"
                };
                Children.Add(newIFolder);
            }

        }

        internal void ExpandChildren()
        {
            foreach (var value in Children)
            {
                try
                {
                    Folder folder = (Folder)value;
                    folder.IsExpanded = true;
                    folder.GetChildren();

                }
                catch (Exception e)
                {

                }
                
            }
        }
    }
}