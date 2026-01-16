using System.Data.OleDb;
using System.Diagnostics;
using System.Runtime.CompilerServices;
            //    using (OleDbDataAdapter = new OleDbDataAdapter())
            //        string[] arr =
            var i = 0;
            Form form = new Form();

            Button btn = new Button
            {
                Text = "显示i当前数字",
                AutoSize = true
            };

            Button btn1 = new Button
                Text = "关闭窗体",
                AutoSize = true
            btn.Click += (sender, eventArgs) => 
            {
                MessageBox.Show(i.ToString());
                i++;
            };
            btn1.Click += (sender, eventArgs) => form.Close();
            form.Controls.Add(btn);
            form.Controls.Add(btn1);
            form.Controls[1].Location = new System.Drawing.Point(100, 0);
            form.ShowDialog();

            for (; i < 1000; i++)
            {
                Debug.Print(i.ToString());
            }
            form.ShowDialog();
            
﻿namespace 单据汇总
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.button1 = new System.Windows.Forms.Button();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.button2 = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(212, 114);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 0;
            this.button1.Text = "确定";
            this.button1.UseVisualStyleBackColor = true;
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(198, 69);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(100, 21);
            this.textBox1.TabIndex = 1;
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(315, 114);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(75, 23);
            this.button2.TabIndex = 0;
            this.button2.Text = "显示值";
            this.button2.UseVisualStyleBackColor = true;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(423, 214);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.button1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Button button2;
    }
}
﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace 单据汇总
{
    public partial class Form1 : Form
    {
        public event Action<string> ConfirmClick; 
        public Form1()
        {
            InitializeComponent();
            button1.Click += (sender, args) => ConfirmClick.Invoke(textBox1.Text);
        }
    }
}
using System.Windows.Forms;
using Excel = Microsoft.Office.Interop.Excel;
using Tools = Microsoft.Office.Tools;
        public static Form1 form;
         static void Main(string[] args)
         {
            
            OperationExcel();


            //var orderList = GetOrderList();

            //var viewData = orderList.GroupBy(e => new { e.BillType, e.ShopName });
            //foreach (var item in viewData.Select(e => e.Key))
            //{
            //    var e = viewData.FirstOrDefault(x => x.Key.BillType == item.BillType && x.Key.ShopName == item.ShopName);
            //    var min = e.Min(x => x.RightNum);
            //    var max = e.Max(x => x.RightNum);
            //    var result = "";
            //    var data = e.OrderBy(x => x.RightNum).ToArray();
            //    var index = 0;
            //    for (var i = min; i < max; i++)
            //    {
            //        if (data[index].RightNum != i)
            //        {
            //            result = data[index - 1].RightNum == min ? $"{result}{min}," : $"{result}{min}-{data[index - 1].RightNum},";
            //            min = data[index].RightNum;
            //            i = min;
            //        }
            //        index++;
            //    }
            //    result = min == max ? $"{max}" : $"{result}{min}-{max}";
            //    var str = $"订单类型：{e.Key.BillType}，店铺名称：{e.Key.ShopName},单据号：{result}";
            //    Console.WriteLine(str);
            //  return new { e.Key.BillType, e.Key.ShopName, section = result.TrimEnd(',') };

            //}

        #region 操作excel

        public static void OperationExcel()
            Excel.Application app = new Excel.Application();
            
            app.Workbooks.Add();
            app.DisplayDocumentActionTaskPane = true;
            Excel.Worksheet sht = app.ActiveSheet;
            
            #region 写入数据

            object[,] values = new object[2, 3] { { "test", "", null }, { "序号", "名称", "描述" } };
            sht.get_Range("A1", "C2").Value2 = values;
            int x = values.GetLength(1);
            int low = values.GetLowerBound(0);
            int up = values.GetUpperBound(1);
            for (int i = 0; i < x; i++)
                Debug.Print(values[1,i].ToString());
            #endregion


            app.Visible = true;
        }


        #endregion

        #region 测试函数
        /// <summary>
        /// 获取数据并进行单号汇总
        /// </summary>
        /// <returns></returns>
        public static  IEnumerable<OrderTotal> GetOrderList()
        {
            var ceshi = "";

            #region 连接数据库并获取数据

            //var conn = new OleDbConnection(@"Provider=Microsoft.Ace.OleDb.12.0;Extended Properties=Excel 12.0;Data Source=C:\Users\Administrator\Desktop\单据审核中心.xls");
            //string str = "select 单据类型, " +
            //             "IIF(INSTR(单位名称, '/') > 0, MID(单位名称, 1, INSTR(单位名称, '/') -1), 单位名称) AS 单位名称," +
            //             " CINT(RIGHT(单号, 3)) AS 单号  from [单据审核中心$]";
            //var dt = new DataTable();
            //using (OleDbDataAdapter da = new OleDbDataAdapter(str, conn))
            //{
            //    da.Fill(dt);
            //}

            #endregion
            #region 测试窗体是否能阻止继续执行代码

            //var tmp = dt.AsEnumerable().Select(e =>
            //{
            //    var tmp1 = "";
            //    ceshi = e.ItemArray[2].ToString();
            //    if (e.ItemArray[0].ToString() == "盘点单")
            //    {

            //        tmp1 = Test(e[0].ToString() + "22");

            //    }

            //    return tmp1== "" ? e.ItemArray[0] : tmp1;
            //});
            


            #endregion

            #region 单号汇总

            //var viewData = dt.AsEnumerable().GroupBy(x => new {单据类型 = x[0], 单位名称 = x[1]}, x=> x[2]).
            //    Select(x =>
            //    {
            //        //var dwmc = x.Key.单位名称;
            //        var data = Array.ConvertAll<object, int>(x.ToArray(), e => int.Parse(e.ToString()));
            //        var max = data.Max();
            //        var min = data.Min();
            //        var index = 0;
            //        var result = "";
            //        for (var i = min; i < max; i++)
            //        {
            //            if(data[index] != i)
            //            {
            //                result = data[index - 1] == min ? $"{result}{min}," : $"{result}{min}-{data[index - 1]},";
            //                min = data[index];
            //                i = min;
            //            }
            //            index++;
            //        }
            //        result = min == max ? $"{result}{max}" : $"{result}{min}-{max}";

            //        return new{ x.Key.单据类型, x.Key.单位名称, result };
            //    });

            #endregion
            //string[] ddd = new[] {"柯桥爱尚美化妆品", "⊙唐三彩武林2店", "⊙唐三彩下沙保利湾店"};
            //var test = viewData.Select(e =>
            //{
            //    var cc1 = e.单位名称;
            //    var aa = ddd.FirstOrDefault(a => a.ToString() == e.单位名称.ToString());
            //    Debug.Print(aa ?? $"{cc1}没有找到");
            //    return new {e.单据类型, cc = aa + cc1 + "哈哈", e.result };
            //});
            
            return null;
        }

        #endregion

        #region 事件测试
        /// <summary>
        /// 事件测试
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
         public static  string Test(string str)
        {
            var tmp1 = "";
            if (form == null)
                form = new Form1();
                form.ConfirmClick += s =>
                {
                    if (string.IsNullOrEmpty(s))
                    {
                        MessageBox.Show(@"不得为空");
                        return;
                    }
                    tmp1 = s;
                    
                    form.Hide();
                };
            form.ShowDialog();
            return tmp1;

        #endregion
       



    
