using System;
using System.ComponentModel;
using System.Drawing;
using System.Threading;
using System.Windows.Forms;

namespace BackgroundWorkerExample
{

    public class Form1 : Form
    {
        BackgroundWorker backgroundWorker1 = new BackgroundWorker();
        Button startBtn = new Button() { Location = new Point(10, 10), Size = new Size(75, 23), Text = "Start" };
        Button cancelBtn = new Button() { Location = new Point(100, 10), Size = new Size(75, 23), Text = "Cancel" };
        Label labelLbl = new Label() { Location = new Point(10, 60), Size = new Size(95, 23), Text = "-", Font = new Font("Verdana", 12) };

		[STAThread]
		static void Main()
		{
			Application.EnableVisualStyles();
			Application.Run(new Form1());
		}

        public Form1()
        {
            backgroundWorker1.WorkerReportsProgress = true;
            backgroundWorker1.WorkerSupportsCancellation = true;
            backgroundWorker1.DoWork             += new DoWorkEventHandler(DoWork);
            backgroundWorker1.RunWorkerCompleted += new RunWorkerCompletedEventHandler(RunWorkerCompleted);
            backgroundWorker1.ProgressChanged    += new ProgressChangedEventHandler(ProgressChanged);

            startBtn.Click  += new System.EventHandler(startBtn_Click);
            cancelBtn.Click += new System.EventHandler(cancelBtn_Click);

            ClientSize = new Size(190, 100);

            Controls.Add(cancelBtn);
            Controls.Add(startBtn);
            Controls.Add(labelLbl);
        }

        void startBtn_Click(object sender, EventArgs e)
        {
            if ( backgroundWorker1.IsBusy != true )
                backgroundWorker1.RunWorkerAsync();
            else
                MessageBox.Show("The worker is running...");
        }

        void cancelBtn_Click(object sender, EventArgs e)
        {
            if ( backgroundWorker1.IsBusy == true && backgroundWorker1.WorkerSupportsCancellation == true ) 
                backgroundWorker1.CancelAsync();
            else
                MessageBox.Show("There isn't worker...");
        }

        void DoWork(object sender, DoWorkEventArgs e)
        {
			int counter = 50;
            for (int i = 1; i <= counter; i++)
            {
                BackgroundWorker worker = sender as BackgroundWorker;
				if (worker.CancellationPending == true)
                {
                    e.Cancel = true;
                    break;
                }
                else
                {
                    System.Threading.Thread.Sleep(500);
                    worker.ReportProgress(i * 100 / counter);
                }
            }
        }

        void RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled == true)
            {
                labelLbl.Text = "Canceled!";
            }
            else if (e.Error != null)
            {
                labelLbl.Text = "Error: " + e.Error.Message;
            }
            else
            {
                labelLbl.Text = "Done!";
            }
        }
 
        private void ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            labelLbl.Text = e.ProgressPercentage.ToString() + "%";
        }

    }

}