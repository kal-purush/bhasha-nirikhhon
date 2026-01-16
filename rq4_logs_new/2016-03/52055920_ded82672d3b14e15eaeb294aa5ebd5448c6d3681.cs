using PerformanceEvaluation.Cmn;
using PerformanceEvaluation.PerformanceEvaluation.Info;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace PerformanceEvaluation.PerformanceEvaluation.Dac
{
    /// <summary>
    /// 数据访问类PersonType。
    /// </summary>
    public class PersonTypeDac
    {
        public PersonTypeDac() { }
        private static PersonTypeDac _instance;
        public PersonTypeDac GetInstance()
        {
            if (_instance == null)
            { _instance = new PersonTypeDac(); }
            return _instance;
        }
        #region  成员方法

        /// <summary>
        /// 增加一条数据
        /// </summary>
        public int Add(PersonTypeEntity model)
        {
            SqlCommand cmd = new SqlCommand();
            StringBuilder strSql = new StringBuilder();
            StringBuilder strSql1 = new StringBuilder();
            StringBuilder strSql2 = new StringBuilder();
            strSql.Append("insert into dbo.PersonType(");
            if (model.TypeName != AppConst.StringNull)
            {
                strSql1.Append("TypeName,");
                strSql2.Append("@TypeName,");
                SqlParameter param = new SqlParameter("@TypeName", SqlDbType.NVarChar, 64);
                param.Value = model.TypeName;
                cmd.Parameters.Add(param);
            }
            if (model.Status != AppConst.IntNull)
            {
                strSql1.Append("Status,");
                strSql2.Append("@Status,");
                SqlParameter param = new SqlParameter("@Status", SqlDbType.SmallInt, 2);
                param.Value = model.Status;
                cmd.Parameters.Add(param);
            }
            if (model.CreateTime != AppConst.DateTimeNull)
            {
                strSql1.Append("CreateTime,");
                strSql2.Append("@CreateTime,");
                SqlParameter param = new SqlParameter("@CreateTime", SqlDbType.DateTime);
                param.Value = model.CreateTime;
                cmd.Parameters.Add(param);
            }
            if (model.LastUpdateTime != AppConst.DateTimeNull)
            {
                strSql1.Append("LastUpdateTime,");
                strSql2.Append("@LastUpdateTime,");
                SqlParameter param = new SqlParameter("@LastUpdateTime", SqlDbType.DateTime);
                param.Value = model.LastUpdateTime;
                cmd.Parameters.Add(param);
            }
            if (model.CreateUserSysNo != AppConst.IntNull)
            {
                strSql1.Append("CreateUserSysNo,");
                strSql2.Append("@CreateUserSysNo,");
                SqlParameter param = new SqlParameter("@CreateUserSysNo", SqlDbType.Int, 4);
                param.Value = model.CreateUserSysNo;
                cmd.Parameters.Add(param);
            }
            if (model.LastUpdateUserSysNo != AppConst.IntNull)
            {
                strSql1.Append("LastUpdateUserSysNo,");
                strSql2.Append("@LastUpdateUserSysNo,");
                SqlParameter param = new SqlParameter("@LastUpdateUserSysNo", SqlDbType.Int, 4);
                param.Value = model.LastUpdateUserSysNo;
                cmd.Parameters.Add(param);
            }
            strSql.Append(strSql1.Remove(strSql1.Length - 1, 1)).Append(")");
            strSql.Append(" values (");
            strSql.Append(strSql2.Remove(strSql2.Length - 1, 1)).Append(")");
            strSql.Append(";select @@IDENTITY");
            cmd.CommandText = strSql.ToString();

            return Convert.ToInt32(SqlHelper.ExecuteScalar(AppConfig.Conn_PerformanceEvaluation, cmd));
        }
        /// <summary>
        /// 更新一条数据
        /// </summary>
        public int Update(PersonTypeEntity model)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.Append("update dbo.PersonType set ");
            SqlCommand cmd = new SqlCommand();
            if (model.SysNo != AppConst.IntNull)
            {
                SqlParameter param = new SqlParameter("@SysNo", SqlDbType.Int, 4);
                param.Value = model.SysNo;
                cmd.Parameters.Add(param);
            }
            if (model.TypeName != AppConst.StringNull)
            {
                strSql.Append("TypeName=@TypeName,");
                SqlParameter param = new SqlParameter("@TypeName", SqlDbType.NVarChar, 64);
                param.Value = model.TypeName;
                cmd.Parameters.Add(param);
            }
            if (model.Status != AppConst.IntNull)
            {
                strSql.Append("Status=@Status,");
                SqlParameter param = new SqlParameter("@Status", SqlDbType.SmallInt, 2);
                param.Value = model.Status;
                cmd.Parameters.Add(param);
            }
            if (model.CreateTime != AppConst.DateTimeNull)
            {
                strSql.Append("CreateTime=@CreateTime,");
                SqlParameter param = new SqlParameter("@CreateTime", SqlDbType.DateTime);
                param.Value = model.CreateTime;
                cmd.Parameters.Add(param);
            }
            if (model.LastUpdateTime != AppConst.DateTimeNull)
            {
                strSql.Append("LastUpdateTime=@LastUpdateTime,");
                SqlParameter param = new SqlParameter("@LastUpdateTime", SqlDbType.DateTime);
                param.Value = model.LastUpdateTime;
                cmd.Parameters.Add(param);
            }
            if (model.CreateUserSysNo != AppConst.IntNull)
            {
                strSql.Append("CreateUserSysNo=@CreateUserSysNo,");
                SqlParameter param = new SqlParameter("@CreateUserSysNo", SqlDbType.Int, 4);
                param.Value = model.CreateUserSysNo;
                cmd.Parameters.Add(param);
            }
            if (model.LastUpdateUserSysNo != AppConst.IntNull)
            {
                strSql.Append("LastUpdateUserSysNo=@LastUpdateUserSysNo,");
                SqlParameter param = new SqlParameter("@LastUpdateUserSysNo", SqlDbType.Int, 4);
                param.Value = model.LastUpdateUserSysNo;
                cmd.Parameters.Add(param);
            }
            strSql.Remove(strSql.Length - 1, 1);
            strSql.Append(" where SysNo=@SysNo ");
            cmd.CommandText = strSql.ToString();
            return SqlHelper.ExecuteNonQuery(AppConfig.Conn_PerformanceEvaluation, cmd);
        }
        /// <summary>
        /// 删除一条数据
        /// </summary>
        public int Delete(int SysNo)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.Append("delete PersonType ");
            strSql.Append(" where SysNo=@SysNo");
            SqlCommand cmd = new SqlCommand(strSql.ToString());
            SqlParameter[] parameters = { new SqlParameter("@SysNo", SqlDbType.Int, 4) };
            parameters[0].Value = SysNo;
            cmd.Parameters.Add(parameters[0]);
            return SqlHelper.ExecuteNonQuery(AppConfig.Conn_PerformanceEvaluation, cmd);
        }
        /// <summary>
        /// 将DataRow赋值到实体
        /// </summary>
        private PersonTypeEntity SetDsToEntity(DataSet ds, PersonTypeEntity model)
        {
            if (ds.Tables[0].Rows[0]["SysNo"].ToString() != "")
            {
                model.SysNo = int.Parse(ds.Tables[0].Rows[0]["SysNo"].ToString());
            }
            model.TypeName = ds.Tables[0].Rows[0]["TypeName"].ToString();
            if (ds.Tables[0].Rows[0]["Status"].ToString() != "")
            {
                model.Status = int.Parse(ds.Tables[0].Rows[0]["Status"].ToString());
            }
            if (ds.Tables[0].Rows[0]["CreateTime"].ToString() != "")
            {
                model.CreateTime = DateTime.Parse(ds.Tables[0].Rows[0]["CreateTime"].ToString());
            }
            if (ds.Tables[0].Rows[0]["LastUpdateTime"].ToString() != "")
            {
                model.LastUpdateTime = DateTime.Parse(ds.Tables[0].Rows[0]["LastUpdateTime"].ToString());
            }
            if (ds.Tables[0].Rows[0]["CreateUserSysNo"].ToString() != "")
            {
                model.CreateUserSysNo = int.Parse(ds.Tables[0].Rows[0]["CreateUserSysNo"].ToString());
            }
            if (ds.Tables[0].Rows[0]["LastUpdateUserSysNo"].ToString() != "")
            {
                model.LastUpdateUserSysNo = int.Parse(ds.Tables[0].Rows[0]["LastUpdateUserSysNo"].ToString());
            }
            return model;
        }
        /// <summary>
        /// 得到一个对象实体
        /// </summary>
        public PersonTypeEntity GetModel(int SysNo)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.Append("select *  from  dbo.PersonType");
            strSql.Append(" where SysNo=@SysNo ");
            SqlParameter[] parameters = { 
		new SqlParameter("@SysNo", SqlDbType.Int,4 )
 		};
            parameters[0].Value = SysNo;
            PersonTypeEntity model = new PersonTypeEntity();
            DataSet ds = SqlHelper.ExecuteDataSet(AppConfig.Conn_PerformanceEvaluation, strSql.ToString(), parameters);
            if (ds.Tables[0].Rows.Count > 0)
            {
                SetDsToEntity(ds, model);
                return model;
            }
            else
            {
                return null;
            }
        }
        #endregion  成员方法

        public PersonTypeEntity SetDrToEntity(DataRow dr, PersonTypeEntity model)
        {
            if (dr["SysNo"].ToString() != "")
            {
                model.SysNo = int.Parse(dr["SysNo"].ToString());
            }
            model.TypeName = dr["TypeName"].ToString();
            if (dr["Status"].ToString() != "")
            {
                model.Status = int.Parse(dr["Status"].ToString());
            }
            if (dr["CreateTime"].ToString() != "")
            {
                model.CreateTime = DateTime.Parse(dr["CreateTime"].ToString());
            }
            if (dr["LastUpdateTime"].ToString() != "")
            {
                model.LastUpdateTime = DateTime.Parse(dr["LastUpdateTime"].ToString());
            }
            if (dr["CreateUserSysNo"].ToString() != "")
            {
                model.CreateUserSysNo = int.Parse(dr["CreateUserSysNo"].ToString());
            }
            if (dr["LastUpdateUserSysNo"].ToString() != "")
            {
                model.LastUpdateUserSysNo = int.Parse(dr["LastUpdateUserSysNo"].ToString());
            }
            return model;
        }
    }
}