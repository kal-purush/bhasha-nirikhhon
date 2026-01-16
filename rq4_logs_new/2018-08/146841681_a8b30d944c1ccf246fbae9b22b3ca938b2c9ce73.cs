using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace newbrewers
{
    /// <summary>
    /// 酒類等製造免許の新規取得者。
    /// あまり気にせず
    /// </summary>
    class Brewer
    {
        /// <summary>
        /// prefecture:都道府県, taxoffice:税務署名, licenceDate:免許等年月日 ,registDate:申請等年月日 ,name:製造者氏名又は名称 ,place:製造場所在地 ,classification:免許等区分 ,item:品目 ,type:処理区分
        /// </summary>
        public enum RegistInfo { prefecture, taxoffice, licenceDate, registDate, name, place, classification, item, type };
        public string[] brewer;

        public Brewer(string pref, string[] info)
        {
            brewer = new string[info.Length + 1];
            brewer[0] = pref;
            for(var i = 0; i < info.Length; i++)
            {
                brewer[i + 1] = info[i];
            }
        }

        public string getParam(RegistInfo n)
        {
            return brewer[(int)n];
        }

        public override string ToString()
        {
            return " " + string.Join(" , ", brewer);
        }
    }
}