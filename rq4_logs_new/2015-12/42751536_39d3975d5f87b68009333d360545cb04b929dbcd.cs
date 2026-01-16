using ArkWrap;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace WpfApplication1
{
    /// <summary>
    /// 기본적으로 메모리 스트림을 가지고, 파일로 쓰기위한 기능을 갖춤
    /// </summary>
    public class MemoryFileStream : MemoryStream
    {
        public int Index { private set; get; }
        public long Size { private set; get; }
        public string Fullname { private set; get; }
        public string Name { private set; get; }
        public string Ext { private set; get; }

        public MemoryFileStream(int index, Ark.SArkFileItem item)
        {
            Fullname = item.Filename;
            Ext = Path.GetExtension(Fullname).Skip(1).ToString();
            Name = Path.GetFileNameWithoutExtension(Regex.Replace(Fullname, "^[^\n]+\\\\", ""));
            Size = item.uncompressedSize;
            this.Index = index;
        }

        public void ExtChange(string ext)
        {
            this.Ext = ext;
        }

        public void ToFile(string path)
        {
         // 파일에 쓰기
            using (var file = new FileStream(path + Path.DirectorySeparatorChar + Name + "." + Ext, FileMode.CreateNew, FileAccess.Write))
            {
                Seek(0, SeekOrigin.Begin);
                CopyTo(file);
                Dispose();
            }
        }
        public override string ToString()
        {
            return "Index : " + this.Index + "/ " + Name + "." + Ext;
        }
    }

    public class BWork : SingletonBase<BWork>
    {
        private int progress;
        private int progressMax;
        public double Progress { get { return Math.Round((double)(progress * 100) / progressMax); } }

        public Dictionary<int, Ark.SArkFileItem> FileItems { private set; get; }
        public MemoryFileStream Cursor { private set; get; }
    
        private Queue<MemoryFileStream> Files;

        public void Load(string path, string pass = null)
        {
            if (!File.Exists(path))
                throw new ArgumentException("Is Not Exist File");

            Ark.ARKERR err = Ark.Create();
            if (err != Ark.ARKERR._NOERR)
                throw new Exception(err.ToString());
            
            using (FileStream file = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                byte[]  bytes = new byte[file.Length];
                file.Read(bytes, 0, (int)file.Length);

                GCHandle rawDataHandle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                IntPtr address = rawDataHandle.AddrOfPinnedObject();

            //if(!Ark.OpenStream(address, bytes.Length))
            //    throw new FileLoadException("File Open Error" + Ark.GetLastErrorArk().ToString());
            }

            if (Ark.IsBrokenArchive())
                throw new Exception("IsBrokenArchive" + Ark.GetLastErrorArk().ToString());

            var length = Ark.GetFileItemCount();
            FileItems = new Dictionary<int, Ark.SArkFileItem>(length);
            for (int i = 0; i < length; i++)
            {
                var result = Ark.GetFileItem(i);
                //FileItems.Add(i, (Ark.SArkFileItem)Marshal.PtrToStructure(ptr, typeof(Ark.SArkFileItem)));
            }

            Files = new Queue<MemoryFileStream>(FileItems.Count());

            
            ArkOutStream.Instance.Open = eOpen;
            ArkOutStream.Instance.CreateFolder = eFolder;
            ArkOutStream.Instance.SetSize = eSize;
            ArkOutStream.Instance.Write = eWrite;
            ArkOutStream.Instance.Close = eClose;
        }

        public void ExtractOne(int index)
        {
            if (Files.Any(x => x.Index == index))
            {
                Cursor = Files.First(x => x.Index == index);
                return;
            }

            Cursor = new MemoryFileStream(index, FileItems[index]);

            Ark.ExtractOneToStream(index);
        }

        public bool eOpen(string path)
        {
            Console.WriteLine("Open : " + path);
            return true;
        }

        public bool eFolder(string path)
        {
            Console.WriteLine("Folder : " + path);
            return true;
        }

        public bool eSize(ulong size)
        {
            Console.WriteLine("Size : " + size);
            return true;
        }

        public bool eWrite(IntPtr ptr, uint size)
        {
            Console.WriteLine("Write : " + size);
            try
            {
                byte[] buffer = new byte[size];
                Marshal.Copy(ptr, buffer, 0, buffer.Length);
                Cursor.SetLength(Cursor.Length + size);
                Cursor.Write(buffer, 0, buffer.Length);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool eClose()
        {
            Console.WriteLine("Close");
            Console.WriteLine(Cursor.Name);
            return true;
        }
    }

    public class BatchUtil
    {
        /// <summary>
        /// 파일명에서 특정 단어를 제거
        /// </summary>
        /// <param name="input"></param>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public static string GetPatternRemove(ref string input, string pattern)
        {
            var match = Regex.Match(input, pattern);
            if (match.Success)
            {
                input = input.Replace(match.Value, "").Trim();
                return match.Value;
            }
            return null;
        }

        /// <summary>
        /// 여러개의 특정 단어를 제거
        /// </summary>
        /// <param name="str"></param>
        /// <param name="patterns"></param>
        /// <returns></returns>
        public static string ReplaceMulti(string str, string[] patterns)
        {
            foreach (var item in patterns)
            {
                str = str.Replace(item, "");
            }
            str.Trim();
            return str;
        }

        /// <summary>
        /// 휴지통으로 보내기
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        public static bool GoRecycle(string target)
        {
            if (string.IsNullOrEmpty(target))
                throw new ArgumentNullException();

            if (File.Exists(target))
                Microsoft.VisualBasic.FileIO.FileSystem.DeleteFile(target, Microsoft.VisualBasic.FileIO.UIOption.OnlyErrorDialogs, Microsoft.VisualBasic.FileIO.RecycleOption.SendToRecycleBin);
            else if (Directory.Exists(target))
                Microsoft.VisualBasic.FileIO.FileSystem.DeleteDirectory(target, Microsoft.VisualBasic.FileIO.UIOption.OnlyErrorDialogs, Microsoft.VisualBasic.FileIO.RecycleOption.SendToRecycleBin);
            else
                throw new FileNotFoundException();

            return true;
        }

    }
}