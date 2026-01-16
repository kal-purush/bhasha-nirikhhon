using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using CP_MockTest_DLL;

namespace WindowsFormsApp1
{
    public partial class Form1 : Form
    {

        static object _lock = new object();

        CountdownEvent cde = new CountdownEvent(5);

        FileServer fs;

        SemaphoreSlim s = new SemaphoreSlim(5, 5);

        ReaderWriterLock rwl = new ReaderWriterLock();

        public Form1()
        {
            InitializeComponent();

            fs = FileServer.GetInstance();

        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            init();
        }

        void init() {

            btnStart.Enabled = false;

            for (int i = 1; i <= 5; i++)
            {
                Thread t = new Thread(get);
                t.Start(i);
            }

            new Thread(() => {
                cde.Wait();

                invoke(() => {
                    btnStart.Enabled = true;

                    //readAndLoad();
                    readAndWrite();

                });

                MessageBox.Show("threads finished");
            }).Start();

        }

        void readAndLoad() { 
            
            List<string> msgs = new List<string>();

            // write your own logic for reading a file

            for (int i = 0; i < 10; i++)
            {
                msgs.Add(i.ToString());
            }

            invoke(() => {
                foreach (var item in msgs)
                {
                    listBox2.Items.Add(item);
                }
            });

        }

        void readAndWrite() {

            int i = 1;

            while (i <= 100) {
                new Thread(
                    () =>
                    {

                        rwl.AcquireWriterLock(Timeout.Infinite);

                        // write your own logic for writing to a file

                        Console.WriteLine("Write operation " + i);

                        rwl.ReleaseWriterLock();

                        lock (_lock) {
                            i++;
                        }
                    
                    }
                ).Start();
            }

        }

        void get(object i) {

            s.Wait();

            int id = (int) i;

            // thread start msg

            invoke(() => {
                listBox1.Items.Add("Thread " + id + " started");
            });

            // ws get msg

            byte[] array = fs.GetFile(id);

            string msg = Encoding.Default.GetString(array);

            try
            {
                // log msg
                invoke(() => {
                    listBox2.Items.Add(msg);
                });

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                
            }
            finally {

                cde.Signal();

                Console.WriteLine(cde.CurrentCount);

                s.Release();

            }

            // thread finished msg

            invoke(() => {
                listBox1.Items.Add("Thread " + id + " finished");
            });
            
        }

        void invoke(Action func) {
            Invoke(
                new MethodInvoker(
                    () => {
                        func();
                    }
                )    
            );
        }

    }
}