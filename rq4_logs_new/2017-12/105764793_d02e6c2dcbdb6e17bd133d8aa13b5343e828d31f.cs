//
//  FileDropBoard - v 1.0
//    by 2012 - 2017 Hiro-Project
//  GitHub : https://github.com/hiro0916-ptcl/WF-FileDropBoard 
//
//  LICENSED under the GPL-3.0
//   - See : https://www.gnu.org/licenses/gpl-3.0.en.html
//

using System.Text;
using Newtonsoft.Json.Linq;
using System.IO;
using System;
using System.Windows.Forms;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace WF_FileDropBoard
{
    /// <summary>
    /// 設定を読み込んだり書き込んだりするクラス。
    /// </summary>
    public class DataIO {
        /// <summary>
        /// ディスクに JSON として設定を書き込みます。
        /// </summary>
        /// <param name="FilePath">ファイルのパス。</param>
        /// <param name="settings">書き込む Configuration クラス。</param>
        public static void SaveSettings(string FilePath, Configuration settings) {
            var dc = new Dictionary<string, object>() {
                { "File_AlwaysShowDate",settings.File_AlwaysShowDate },
                { "File_ShowDateMode",settings.File_ShowDateMode },
                { "File_ShowPreview",settings.File_ShowPreview },
                { "FileListS",settings.FileListS} ,
                { "ExtCol",settings.ExtCol}
            };
            using (StreamWriter SR = new StreamWriter(FilePath, false, Encoding.UTF8)) {
                SR.WriteLine(JsonConvert.SerializeObject(dc));
            }
        }

        /// <summary>
        /// ディスクから XML として保存された設定を読み込みます。
        /// </summary>
        /// <typeparam name="T">書き込むクラスの型。</typeparam>
        /// <param name="FilePath">ファイルのパス。</param>
        /// <returns></returns>
        public static Configuration LoadSettings(string FilePath) {
            JObject JO = new JObject();
            Configuration cf = new Configuration(null);
            try {
                using (StreamReader SR = new StreamReader(FilePath, Encoding.UTF8)) {
                    string dat = SR.ReadToEnd();
                    JO = JObject.Parse(dat);
                }
                var dc = new Dictionary<string, object>();
                cf.File_AlwaysShowDate = (bool)dc["File_AlwaysShowDate"];
                cf.File_ShowDateMode = (Main.File_ShowDateModeE)dc["File_ShowDateMode"];
                cf.File_ShowPreview = (bool)dc["File_ShowPreview"];
                cf.FileListS = (List<FData.FileData>)dc["FileListS"];
                cf.ExtCol = (Dictionary<String, System.Drawing.Color>)dc["ExtCol"];

                return cf;
            } catch (Exception e) {
                MessageBox.Show(
                    "設定が正しく読み込めなかったため、\n" +
                    "現在の設定ファイルを再生成します。\n" +
                    "読み込めなかった設定ファイルは\n" +
                    FilePath + ".old \n" +
                    "として保存されます。" ,
                    "エラー - FileDropBoard" ,
                    MessageBoxButtons.OK ,
                    MessageBoxIcon.Information
                );
                File.Copy(FilePath, FilePath + ".old");
                File.Delete(FilePath);
                return new Configuration(null);
            }
            return null;
        }
    }
}

// By the way, do you like PINEAPPLES? I love it! xD
// [EOF]