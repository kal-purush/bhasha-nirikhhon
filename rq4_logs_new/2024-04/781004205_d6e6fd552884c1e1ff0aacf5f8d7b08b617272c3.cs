using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DoAnCuoiKy
{
    internal class CandidateDAO
    {
        DBconnection db = new DBconnection();
        SqlConnection connStr = new SqlConnection(DoAnCuoiKy.Properties.Settings.Default.connStr);
        FFindingCandidate FFindingCandidate = new FFindingCandidate();
        public void them(Candidate candidate)
        {
            string SQL = string.Format("INSERT INTO Profile ( ID,  Name,  gender,  nationality,  address,  DOB,  ExpYears,  email,  phone,  describe, ApplyPosition, Skill) VALUES ('{0}', '{1}' , '{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')",
                candidate.ID, candidate.Name, candidate.Gender,
                candidate.Nationality, candidate.Address, candidate.DOB, candidate.ExpYears,
                candidate.Email,candidate.Phone,candidate.Describe,candidate.ApplyPosition,candidate.Skill);
            db.thucthi(SQL);
            

        }
        public void LoadDanhSach(FApplicants f) 
        {
            List<Candidate> list = new List<Candidate>();
            try
            {
                string query = "SELECT * FROM Profile";
                SqlCommand command = new SqlCommand(query, connStr);
                connStr.Open();
                SqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    string ID = reader["ID"].ToString();
                    string Name = reader["Name"].ToString();
                    string gender = reader["Gender"].ToString();
                    string nationality = reader["Nationality"].ToString();
                    string address = reader["Address"].ToString();
                    string DOB = reader["DOB"].ToString();
                    string expYears = reader["ExpYears"].ToString();
                    string email = reader["Email"].ToString();
                    string phone = reader["Phone"].ToString();
                    string describe = reader["Describe"].ToString();
                    string applyPos = reader["ApplyPosition"].ToString();
                    string skill = reader["Skill"].ToString();
                    Candidate candidate = new Candidate(ID, Name, gender, nationality, address, DOB, expYears, email, phone, describe, applyPos, skill);

                    list.Add(candidate);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Lỗi truy vấn: " + ex.Message);
            }
            finally
            {
                connStr.Close();
            }
            foreach (Candidate candidate1 in list)
            {
                UCApplicants uCApplicants = new UCApplicants(candidate1);
                uCApplicants.btnFavorite.Hide();
                f.pnlAllCandidate.Controls.Add(uCApplicants);
            }

        }
        public void xoaUC(UCApplicants uCApplicants)
        {
            string SQL = string.Format("DELETE FROM Profile WHERE  ApplyPosition = '{0}' ",
             uCApplicants.lblCandidateApplyPos.Text);
            db.thucthi(SQL);
        }
    }
}