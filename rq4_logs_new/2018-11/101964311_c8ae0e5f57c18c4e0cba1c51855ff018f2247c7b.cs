using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Algorithm
{
    class DTPractice
    {
        public void Recognize()
        {
            DataTable dt = new DataTable();
            DataTable dt1 = new DataTable("Table_New");
            //创建空列
            DataColumn dc = new DataColumn();
            dt.Columns.Add(dc);

            //创建带列名和类型名的列
            dt.Columns.Add("column0",System.Type.GetType("System.String"));
            dt.Columns.Add("column0", typeof(String));

            //通过列架构添加列
            DataColumn dc1 = new DataColumn("colunm1", System.Type.GetType("System.DateTime"));
            DataColumn dc2 = new DataColumn("colunm1", typeof(DateTime));
            dt.Columns.Add(dc1);

            //创建空行
            DataRow dr = dt.NewRow();
            dt.Rows.Add(dr);
            //创建空行
            dt.Rows.Add();
            //通过行框架创建并赋值
            dt1.Rows.Add("张三", DateTime.Now);
            //通过复制dt1表里的第0行来创建
            dt.Rows.Add(dt1.Rows[1].ItemArray);

            //赋值和取值
            //新建行的赋值
            DataRow dr1 = dt.NewRow();
            //通过索引赋值
            dr1[0] = "张三";
            //通过名称赋值
            dr1["column1"] = DateTime.Now;

            //对表已有行进行赋值
            dt.Rows[0][0] = "李四";
            dt.Rows[0]["column1"] = DateTime.Now;

            //取值
            string name = dt.Rows[0][0].ToString();
            string time = dt.Rows[0]["column1"].ToString();

            //筛选行
            //选择column1列值为空的行的集合
            DataRow[] drs = dt.Select("column1 is null");

            DataRow[] drs1 = dt.Select("column0='李四'");

            DataRow[] drs2 = dt.Select("column0 like '张%'");

            DataRow[] drs3 = dt.Select("column0 like '张%'","column1 DESC");


            //删除行
            dt.Rows.Remove(dt.Rows[0]);
            dt.Rows.RemoveAt(0);
            dt.Rows[0].Delete();
            dt.AcceptChanges();

            //---区别和注意点
            //Remove()和RemoveAt()方法是直接删除
            //Delete()方法只是将该行标记为deleted,但是还存在,还可DataTable.RejectChanges()回滚,使该行取消删除
            //用Rows.Count来获取行数时,还是删除之前的行数,需要使用DataTable.AcceptChanges()方法来提交修改
            //如果要删除DataTable中的多行,应该采用倒序循环DataTable.Rows,而且不能用foreach进行循环删除,因为正序删除时索引会发生变化          
            for (int i = dt.Rows.Count; i >= 0; i--)
            {
                dt.Rows.RemoveAt(i);
            }

            //复制表,同时复制了表结构和表中的数据
            DataTable dtNew = new DataTable();
            dtNew = dt.Copy();
            //复制表
            DataTable dtNew1 = dt.Copy();//复制dt表数据结构
            dtNew1.Clear();//清空数据
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (true)
                {
                    dtNew1.Rows.Add(dt.Rows[i].ItemArray);//添加数据行
                }
            }

            //克隆表,只是复制了表结构,不包括数据
            DataTable dtNew2 = new DataTable();
            dtNew2 = dt.Clone();
            //如果只需要某个表中的某一行
            DataTable dtNew3 = new DataTable();
            dtNew3 = dt.Copy();
            dtNew3.Rows.Clear();//清空表数据
            dtNew3.ImportRow(dt.Rows[0]);//这是加入的第一行

            //表排序
            DataTable dt2 = new DataTable();
            dt2.Columns.Add("ID", typeof(Int32));
            dt2.Columns.Add("Name", typeof(String));
            dt2.Columns.Add("Age", typeof(Int32));
            dt2.Rows.Add(new object[] { 1, "张三", 20 });
            dt2.Rows.Add(new object[] { 2, "李四", 21 });
            dt2.Rows.Add(new object[] { 3, "王五", 22 });
            DataView dv = dt2.DefaultView;//获取表视图
            dv.Sort = "ID DESC";//按照ID倒序排序
            dv.ToTable();//转为表

            dt2.Columns["ID"].ColumnName = "DDD";

           





        }
    }
}