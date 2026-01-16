using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Data.OleDb;
using System.Windows.Forms.DataVisualization.Charting;
using System.Drawing.Text;

namespace SPDRC_PROGRAM
{
    public partial class UserControl_TES_EPD : UserControl
    {
        DataTable dtA;
        string waveLength1 = "";
        string waveLength2 = "";
        string timeRate = "0.1";
        Boolean cbBoxWavelength1Checked = false;
        Boolean cbBoxWavelength2Checked = false;
        Dictionary<string, object> waveLength_thresholdEnergy = new Dictionary<string, object>();
        List<string> list_EPDTime = new List<string>();

        public UserControl_TES_EPD()
        {
            InitializeComponent();

            EPD_chart.Series.Clear();
            //KSPwaveLengthChooseSetting();

        }

        private void KSPwaveLengthChooseSetting()
        {
            string filePath = "kspOES_baseWaveLength.csv";
            string[] lines = System.IO.File.ReadAllLines(filePath);

            if (lines.Length > 0)
            {
                string firstLine = lines[0];

                string[] headerLabels = firstLine.Split(',');

                cbBox_waveLength1.Items.Clear();
                cbBox_waveLength2.Items.Clear();
                cbBox_waveLength1.Items.AddRange(headerLabels);
                cbBox_waveLength2.Items.AddRange(headerLabels);
            }
        }


        private void o_label_Click(object sender, EventArgs e)
        {

        }

        private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
        {

        }

        private void comboBox_co_SelectedIndexChanged(object sender, EventArgs e)
        {

        }

        private void cbBox_waveLength1_SelectedIndexChanged(object sender, EventArgs e)
        {
            waveLength1 = cbBox_waveLength1.SelectedItem.ToString();
            cbBoxWavelength1Checked = true;
            Console.WriteLine(waveLength1);
        }

        private void cbBox_waveLength2_SelectedIndexChanged(object sender, EventArgs e)
        {
            waveLength2 = cbBox_waveLength2.SelectedItem.ToString();
            cbBoxWavelength2Checked = true;
            Console.WriteLine(waveLength2);
        }

        private void btn_FileLoad_Click(object sender, EventArgs e)
        {
            if (cbBoxWavelength1Checked && cbBoxWavelength2Checked)
            {
                Console.WriteLine("btn_Fileload_Clicked");
                OpenFileDialog dialog = new OpenFileDialog();

                // 아래의 if 문을 통해 파일찾기 팝업이 뜸 
                if (dialog.ShowDialog() == DialogResult.OK)
                {
                    string filePath = "";         

                    filePath = dialog.FileName;
                    Console.WriteLine(String.Format("{0} was imported bybtn_Fileload_Click", filePath));

                    checkedListBox_OESdataCollect.Items.Add(filePath, false);

                    if (filePath.EndsWith(".csv"))
                        dtA = CSVdataSortByWaveLengths(filePath);   // 이 함수 쓰려면 csv 파일 맨 윗줄의 cell이 하나라도 비어있으면 안됨.
                    else if (filePath.EndsWith(".xlsx") || filePath.EndsWith(".xls"))
                        dtA = Xlsx_xlsConvertToDataTable(filePath);
                }

                ApplyMovingAverageFilterToWaveLength1And2();

                DrawGraph();
            }
            else
                MessageBox.Show("'파장1' 그리고 '파장2'를 모두 선택해 주십시오.");

        }

        private DataTable CSVdataSortByWaveLengths(string filePath)
        {
            //Int16 headWordIndex_lineNum = 0;
            Int16 headWordIndex_time = 0;
            Int16 headWordIndex_waveLength1 = 0;
            Int16 headWordIndex_waveLength2 = 0;

            DataTable dt = new DataTable();
            string[] lines = System.IO.File.ReadAllLines(filePath);

            if (lines.Length > 0)
            {
                string thirdLine = lines[2]; // 2

                string[] headerLabels = thirdLine.Split(',');

                short columnIndex = 0;
                foreach (string headerWord in headerLabels)   // create header of Datatale
                {
                    if (headerWord == "Time(sec)")
                    {
                        dt.Columns.Add(new DataColumn(headerWord));
                        headWordIndex_time = columnIndex;
                        Console.WriteLine(headWordIndex_time);
                    }
                    else if (headerWord == waveLength1)
                    {
                        dt.Columns.Add(new DataColumn(headerWord));
                        headWordIndex_waveLength1 = columnIndex;
                        Console.WriteLine(headWordIndex_waveLength1);
                    }
                    else if (headerWord == waveLength2)
                    {
                        dt.Columns.Add(new DataColumn(headerWord));
                        headWordIndex_waveLength2 = columnIndex;
                        Console.WriteLine(headWordIndex_waveLength2);
                    }

                    columnIndex++;
                }


                for (int lineNum = 3; lineNum < lines.Length; lineNum++) // fill data to Datatale // 3
                {
                    string[] dataWords = lines[lineNum].Split(',');  // CSV 파일을 row 기준으로 한 줄씩 읽어옴.
                    DataRow dr = dt.NewRow();
                    columnIndex = 0;
                    foreach (string dataWord in dataWords)
                    {
                        if (columnIndex == headWordIndex_time) { dr["Time(sec)"] = dataWord; }
                        else if (columnIndex == headWordIndex_waveLength1) { dr[waveLength1] = (Int32.Parse(dataWord) - 2000).ToString(); } // dataWord  // noise 제거를 위해 raw 데이터에서 2000을 뺌
                        else if (columnIndex == headWordIndex_waveLength2) { dr[waveLength2] = (Int32.Parse(dataWord) - 2000).ToString(); } // dataWord

                        columnIndex++;
                    }
                    dt.Rows.Add(dr);
                }

                //if (dt.Rows.Count > 0)
                //{
                //    dgv_1.DataSource = dt;
                //}

            }

            return dt;
        }

        private DataTable Xlsx_xlsConvertToDataTable(string filePath)
        {
            string dirName = Path.GetDirectoryName(filePath);
            string fileName = Path.GetFileName(filePath);
            string fileExtension = Path.GetExtension(filePath);
            string pathConn = string.Empty;
            string excelsql = string.Empty;

            switch (fileExtension)
            {
                case ".xls":
                    pathConn = $@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={filePath};" + "Extended Properties=\"Excel 8.0; HDR=Yes; IMEX=1\"";
                    excelsql = "SELECT * FROM [Sheet1$]";
                    break;

                case ".xlsx":
                    pathConn = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath + ";Extended Properties='Excel 12.0;HDR=YES'";
                    excelsql = @"select * from[Sheet1$]";
                    break;

                case ".csv":
                    pathConn = $@"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={dirName};" + "Extended Properties=\"text; HDR=Yes; IMEX=1; FMT=Delimited\"";
                    excelsql = $"SELECT * FROM [{fileName}]";
                    break;
            }

            OleDbConnection conn = new OleDbConnection(pathConn);
            OleDbDataAdapter myDataAdapter = new OleDbDataAdapter(excelsql, conn);
            DataSet excelDs = new DataSet();
            myDataAdapter.Fill(excelDs);
            DataTable excelTable = excelDs.Tables[0];

            dgv_1.DataSource = excelTable;

            return excelTable;
        }

        private void ApplyMovingAverageFilterToWaveLength1And2()
        {
           if (!dtA.Columns.Contains("movingAverageFiltered_" + waveLength1) )
           {
                dtA.Columns.Add("movingAverageFiltered_" + waveLength1);
                dtA.Columns.Add("movingAverageFiltered_" + waveLength2);
           }

            for (int rowNum = 0; rowNum <= dtA.Rows.Count - 1; rowNum++)
            {
                //  1/timeRate -2
                if (rowNum <= 8)
                { 
                    dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength1] = dtA.Rows[rowNum][waveLength1];
                    dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength2] = dtA.Rows[rowNum][waveLength2];
                }
                else
                {
                    switch (timeRate)
                    {
                        case "0.1":
                            double sum1 = 0;
                            double sum2 = 0;
                            dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength1] = null;
                            dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength2] = null;

                            //  1/timeRate -1     ,     1/timeRate
                            for (int Num = 0; Num <= 9; Num++)
                            {
                                sum1 += double.Parse(dtA.Rows[rowNum - Num][waveLength1].ToString());  // row[9] ~row[0] 까지 모두 합함
                                sum2 += double.Parse(dtA.Rows[rowNum - Num][waveLength2].ToString());  // row[9] ~row[0] 까지 모두 합함
                            }
                            dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength1] = (sum1 / 10).ToString(); // Sum의 평균을 냄
                            dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength2] = (sum2 / 10).ToString(); // Sum의 평균을 냄

                            break;
                    }
                }
            }

            dgv_1.DataSource = dtA;
        }

        private void DrawGraph()
        {
            this.EPD_chart.Series.Clear(); // 그래프 초기화
            EPD_chart.ChartAreas[0].AxisY.Title = "Intensity";
            EPD_chart.ChartAreas[0].AxisY2.Title = "lineRatio";
            EPD_chart.ChartAreas[0].AxisX.Title = "시간 (sec)";

            Series O = this.EPD_chart.Series.Add("O " + waveLength1);
            O.ChartType = SeriesChartType.Spline;
            O.Color = Color.Red;
            O.MarkerSize = 10; // 선 두께 설정

            Series CO = this.EPD_chart.Series.Add("CO "+waveLength2);
            CO.ChartType = SeriesChartType.Spline;
            CO.MarkerSize = 30; // 선 두께 설정
            CO.Color = Color.Blue;


            for (int rowNum = 0; rowNum <= dtA.Rows.Count - 1; rowNum++)
            {
                CO.Points.AddXY(dtA.Rows[rowNum]["Time(sec)"], dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength1]); //  lineRatio.Points.AddXY(dtA.Rows[rowNum]["Time(sec)"], dtA.Rows[rowNum]["lineRatio"]);
                O.Points.AddXY(dtA.Rows[rowNum]["Time(sec)"], dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength2]); //  Te.Points.AddXY(dtA.Rows[rowNum]["Time(sec)"], dtA.Rows[rowNum]["Te"]);
            }
        }

        private void EPD_chart_Click_1(object sender, EventArgs e)
        {
            EPD_chart.ChartAreas[0].AxisX.ScaleView.Zoomable = true;   // graph zoom 
            EPD_chart.ChartAreas[0].AxisY.ScaleView.Zoomable = true; // graph zoom
            EPD_chart.ChartAreas[0].CursorX.AutoScroll = true;
            EPD_chart.ChartAreas[0].CursorX.IsUserSelectionEnabled = true;
        }

        private void btn_FindEPD_Click(object sender, EventArgs e)
        {
            EPD();
        }

        private void EPD()
        {
            Console.WriteLine("EPD_button_clicked");
            listBox_EPD.Items.Clear();

            if (2 <= dtA.Rows.Count)
            {
                for (int rowNum = 1; rowNum <= dtA.Rows.Count - 1; rowNum++)
                {
                    // 아래는 O와 CO의 기울기가 각각 -1이상 1이하이며, O의 intensity가 14000이상일 때를 조건으로 함     ,   timeRate
                    double CO_gradient = (double.Parse(dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength1].ToString()) - double.Parse(dtA.Rows[rowNum - 1]["movingAverageFiltered_" + waveLength1].ToString())) / 0.2;
                    double O_gradient = (double.Parse(dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength2].ToString()) - double.Parse(dtA.Rows[rowNum - 1]["movingAverageFiltered_" + waveLength2].ToString())) / 0.2;
                    double O_intensity = double.Parse(dtA.Rows[rowNum]["movingAverageFiltered_" + waveLength2].ToString());
                    if (-4.5 <= O_gradient && O_gradient < 4.5  && -4.5 <= CO_gradient && CO_gradient < 4.5 && 14000 <= O_intensity)
                    {
                        listBox_EPD.Items.Add(dtA.Rows[rowNum]["Time(sec)"].ToString());
                    }
                }
            }
            listBox_EPD.Show();

            int typicalRow = 0;
            for (int rowNum = 0; rowNum <= dtA.Rows.Count - 1; rowNum++)
            {
                if (dtA.Rows[rowNum]["Time(sec)"] == listBox_EPD.Items[0])
                { 
                    typicalRow = rowNum;
                    break;
                }

            }

            Series endPoint = this.EPD_chart.Series.Add("End Point");
            endPoint.ChartType = SeriesChartType.Spline;
            endPoint.MarkerSize = 100;
            endPoint.Color = Color.Red;
            endPoint.BorderWidth = 100;
            //endPoint.wid
            string point = dtA.Rows[typicalRow]["movingAverageFiltered_" + waveLength2].ToString();
            endPoint.Points.AddXY(listBox_EPD.Items[0], point);
        }

        private void listBox_EPD_SelectedIndexChanged(object sender, EventArgs e)
        {

        }
    }
}