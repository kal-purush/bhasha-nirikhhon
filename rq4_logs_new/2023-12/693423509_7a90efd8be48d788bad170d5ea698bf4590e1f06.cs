using QLSV.DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QLSV.DAO
{
    internal class lectureRatioScoreDAO
    {
        private static lectureRatioScoreDAO instance;

        public static lectureRatioScoreDAO Instance
        {
            get
            {
                if (instance == null) instance = new lectureRatioScoreDAO();
                return lectureRatioScoreDAO.instance;
            }
            private set
            {
                lectureRatioScoreDAO.instance = value;
            }
        }

        private lectureRatioScoreDAO() { }

        public List<lectureRatioScore> LoadRatioCourse(string id)
        {
            string query = "select * from getRaitoScoreByCourseId( :id )";
            DataTable data = DataProvider.Instance.ExcuteQuery(query, new object[] { id });
            List<lectureRatioScore> scores = new List<lectureRatioScore>();

            foreach (DataRow row in data.Rows)
            {
                lectureRatioScore score = new lectureRatioScore(row);
                scores.Add(score);
            }

            return scores;
        }

        //public bool UpdateRatioScore(string mammon, float diemQT, float diemGK, float diemTH, float diemCK)
        //{
        //    string query = "SELECT UpdateRatioScore( :mamon , :diemQT , :diemGK , :diemTH , :diemCK )";
        //    //bool data = DataProvider.Instance.ExcuteQuery(query, new object[] { mammon, diemQT,diemGK,diemTH,diemCK });
        //    return data;
        //}
    }
}