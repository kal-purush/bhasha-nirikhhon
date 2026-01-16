using System;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Data;

using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;

using GsmComm.PduConverter;
using GsmComm.PduConverter.SmartMessaging;
using GsmComm.GsmCommunication;
using GsmComm.Interfaces;
using GsmComm.Server;
using System.Collections.Generic;
using GSMCommDemo;
using System.Data.OleDb;
using System.Threading;

namespace WindowsFormsApplication2
{
    public partial class Form1 : Form
    {
        private GsmCommMain comm;
        private bool registerMessageReceived;
        private delegate void SetTextCallback(string text);
        string varmessage;
        bool groupsend = false;
        List<int> contactids = new List<int>();
        public Form1()
        {
            InitializeComponent();
            this.comm = null;
            this.registerMessageReceived = false;
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            // Prompt user for connection settings
            string portName = GsmCommMain.DefaultPortName;
            int baudRate = GsmCommMain.DefaultBaudRate;
            int timeout = GsmCommMain.DefaultTimeout;
            connectionFrm dlg = new connectionFrm();
            dlg.StartPosition = FormStartPosition.CenterScreen;
            dlg.SetData(portName, baudRate, timeout);
            if (dlg.ShowDialog(this) == DialogResult.OK)
                dlg.GetData(out portName, out baudRate, out timeout);
            else
            {
                Close();
                return;
            }

            Cursor.Current = Cursors.WaitCursor;
            comm = new GsmCommMain(portName, baudRate, timeout);
            Cursor.Current = Cursors.Default;
            comm.PhoneConnected += new EventHandler(comm_PhoneConnected);
            comm.PhoneDisconnected += new EventHandler(comm_PhoneDisconnected);

            bool retry;
            do
            {
                retry = false;
                try
                {
                    Cursor.Current = Cursors.WaitCursor;
                    comm.Open();
                    MessageRoutingON();
                    Cursor.Current = Cursors.Default;
                }
                catch (Exception)
                {
                    Cursor.Current = Cursors.Default;
                    if (MessageBox.Show(this, "Unable to open the port.", "Error",
                        MessageBoxButtons.RetryCancel, MessageBoxIcon.Warning) == DialogResult.Retry)
                        retry = true;
                    else
                    {
                        Close();
                        return;
                    }
                }
            }
            while (retry);

            // Add custom commands
            ProtocolCommand[] commands = new ProtocolCommand[]
			{
				new ProtocolCommand("Send", true, false, false), // NeedsData
				new ProtocolCommand("Receive", false, false, false),
				new ProtocolCommand("ExecCommand", true, false, false), // NeedsData
				new ProtocolCommand("ExecCommand2", true, false, true), // NeedsData, NeedsError
				new ProtocolCommand("ExecAndReceiveMultiple", true, false, false), // NeedsData
				new ProtocolCommand("ExecAndReceiveAnything", true, true, false), // NeedsData, NeedsPattern
				new ProtocolCommand("ReceiveMultiple", false, false, false),
				new ProtocolCommand("ReceiveAnyhing", false, true, false) // NeedsPattern
			};
           
        }

        private delegate void ConnectedHandler(bool connected);
        private void OnPhoneConnectionChange(bool connected)
        {
            lblNotConnected.Visible = !connected;
        }

        private void OutputCPnumber(string text)
        {
            if (this.cpnumberTxtBox.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputCPnumber);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                cpnumberTxtBox.Text = text;
            }
        }

        private void OutputCode(string text)
        {
            if (this.codeTxtBox.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputCode);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                codeTxtBox.Text = text;
            }
        }

        private void OutputGroup(string text)
        {
            if (this.groupTxtBox.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputGroup);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                groupTxtBox.Text = text;
            }
        }


        private void OutputMessage(string text)
        {
            if (this.txtMessage.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputMessage);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                txtMessage.Text = text;
            }
        }

        private void OutputAreaMobileNumber(string text)
        {
            if (this.areacpnumberTxtBox.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputAreaMobileNumber);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                areacpnumberTxtBox.Text = text;
            }
        }

        public void MessageRoutingON()
        {
          		try
			{
				// Enable direct message routing to the application
				if (!registerMessageReceived)
				{
					comm.MessageReceived += new MessageReceivedEventHandler(comm_MessageReceived);
					registerMessageReceived = true;
				}
				comm.EnableMessageRouting();
			}
			catch(Exception ex)
			{
				Output(ex.ToString());
			}
        }

        private void comm_PhoneConnected(object sender, EventArgs e)
        {
            this.Invoke(new ConnectedHandler(OnPhoneConnectionChange), new object[] { true });
        }

        private void comm_PhoneDisconnected(object sender, EventArgs e)
        {
            this.Invoke(new ConnectedHandler(OnPhoneConnectionChange), new object[] { false });
        }


        int Start = 0;
        int End = 0;
        int i = 0;
        private void btnSendMessage_Click(object sender, EventArgs e)
        {
            Start = 1;
            End = CountContacts();
            i = 0;
            if (radioButton1.Checked == true)
            {
                if (txtMessage.Text != "" && txtNumber.Text != "")
                {
                    if (txtMessage.Text.Length <= 160)
                    {
                        SendMessages(txtNumber.Text);
                    }
                    else
                    {
                        OutgoingSmsPdu[] pdus = CreateConcatMessage(txtMessage.Text, txtNumber.Text);
                        if (pdus != null)
                        {
                            SendMultiple(pdus, txtNumber.Text, "Dashboard Message", 1);
                        }
                    }
                }
            }
            else if (radioButton2.Checked == true)
            {
                if (txtMessage.Text != "")
                {
                    timer1.Start();
                }
            }
            else if (radioButton3.Checked == true)
            {
                if (txtMessage.Text != "")
                {
                    Start = Convert.ToInt32(textBox1.Text);
                    End = Convert.ToInt32(textBox2.Text);
                    timer1.Start();
                }
            }
            else if (radioButton4.Checked == true)
            {
                if (txtMessage.Text != "" && comboBox1.Text != "")
                {
                    groupsend = true;
                    contactids = SendToGroup(comboBox1.Text);
                    timer1.Start();
                }
            }
        }
        private void timer1_Tick(object sender, EventArgs e)
        {
            if (groupsend == true)
            {
                if (i < contactids.Count)
                {
                    if (txtMessage.Text.Length <= 160)
                    {
                        SendMessages(GetContacts(contactids[i]));
                        i++;
                    }
                    else
                    {
                        OutgoingSmsPdu[] pdus = CreateConcatMessage(txtMessage.Text, GetContacts(contactids[i]));
                        if (pdus != null)
                        {
                            SendMultiple(pdus, GetContacts(contactids[i]), "Dashboard Message", i);
                            i++;
                        }
                    }
                }
                else
                {
                    timer1.Stop();
                }
            }
            else
            {
                if (Start <= End)
                {
                    if (txtMessage.Text.Length <= 160)
                    {
                        SendMessages(GetContacts(Start));
                        Start++;
                    }
                    else
                    {
                        OutgoingSmsPdu[] pdus = CreateConcatMessage(txtMessage.Text, GetContacts(Start));
                        if (pdus != null)
                        {
                            SendMultiple(pdus, GetContacts(Start), "Dashboard Message", Start);
                            Start++;
                        }
                    }
                }
                else
                {
                    timer1.Stop();
                }
            }
        }

        public void SendMessages(string cpnumber)
        {
            try
            {
                if (cpnumber != "") {
                    SmsSubmitPdu pdu;
                    pdu = new SmsSubmitPdu(txtMessage.Text, cpnumber);
                    comm.SendMessage(pdu);
                    ShowMessage(pdu);
                    UpdateLog("Sent", "Dashboard Message", cpnumber, txtMessage.Text);
                    Output("[{0}] 1 Message sent to: " + cpnumber, Start);
                    Output("");
                }
            }
            catch (Exception ex)
            {
                UpdateLog("Unsent", "Dashboard Message", cpnumber, txtMessage.Text);
                Output(ex.Message);
                Output("[{0}] 1 Message not sent to: " + cpnumber, Start);
                Output("");
            }
        }

        private OutgoingSmsPdu[] CreateConcatMessage(string textmessage, string mobilenumber)
        {
            OutgoingSmsPdu[] pdus = null;
            try
            {
                pdus = SmartMessageFactory.CreateConcatTextMessage(textmessage, mobilenumber);
            }
            catch (Exception ex)
            {
                Output("Error: " + ex.ToString());
                return null;
            }
            return pdus;
        }

        private void SendMultiple(OutgoingSmsPdu[] pdus, string mobilenumber, string description, int j)
        {
            int num = pdus.Length;
            try
            {
                if (mobilenumber != "")
                {
                    int i = 0;
                    foreach (OutgoingSmsPdu pdu in pdus)
                    {
                        i++;
                        comm.SendMessage(pdu);
                        Output("[" + j + "] " + i.ToString() + "/" + num.ToString() + " Message Sent To: " + mobilenumber);
                        ShowMessage(pdu);
                    }
                    Output(" ");
                    UpdateLog("Sent", description, mobilenumber, txtMessage.Text);
                }
            }
            catch (Exception ex)
            {
                UpdateLog("Unsent", description, mobilenumber, txtMessage.Text);
                Output("1 Message Not Sent To: " + mobilenumber + ". Error: " + ex.Message);
                Output(" ");
            }
        }

        public void UpdateLog(string messagetype, string description, string cpnumber, string message)
        {
            string MyConnectionString;
            string mySQLQuery;
            OleDbCommand myCommand;
            OleDbConnection myConnection;
            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "INSERT INTO [messagelog] ([messagetype], [description], [sender/recepient], [message], [time]) VALUES (@messagetype, @description, @senderrecepient, @message, @time)";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@messagetype", (messagetype)));
                myCommand.Parameters.Add(new OleDbParameter("@description", (description)));
                myCommand.Parameters.Add(new OleDbParameter("@senderrecepient", (cpnumber)));
                myCommand.Parameters.Add(new OleDbParameter("@message", (message)));
                myCommand.Parameters.Add(new OleDbParameter("@time", (String.Format("{0:G}", DateTime.Now))));
                myConnection.Open();
                myCommand.ExecuteNonQuery();
                myCommand.Dispose();
                myConnection.Close();
            }
            catch (Exception ex)
            {
                Output("Error: " + ex.ToString());
            }
        }

        private void Output(string text)
        {
            if (this.txtOutput.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(Output);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                txtOutput.AppendText(text);
                txtOutput.AppendText("\r\n");
            }
        }

        private void OutputSent(string text)
        {
            if (this.txtOutput.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputSent);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                txtbxSent.AppendText(text);
                txtbxSent.AppendText("\r\n");
            }
        }

        private void OutputReceived(string text)
        {
            if (this.txtOutput.InvokeRequired)
            {
                SetTextCallback stc = new SetTextCallback(OutputReceived);
                this.Invoke(stc, new object[] { text });
            }
            else
            {
                txtbxReceived.AppendText(text);
                txtbxReceived.AppendText("\r\n");
            }
        }

        private void Output(string text, params object[] args)
        {
            string msg = string.Format(text, args);
            Output(msg);
        }

        public static int CountContacts()
        {
            int contactcount = 0;

            string MyConnectionString;
            string mySQLQuery = "";
            OleDbCommand myCommand;
            OleDbConnection myConnection;

            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT COUNT(*) FROM [contacts]";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myConnection.Open();
                contactcount = (int) myCommand.ExecuteScalar();
                myConnection.Close();

                return contactcount;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            return 0;
        }

        public string GetContacts(int id)
        {
            string contactnumber = "";
            string MyConnectionString;
            string mySQLQuery = "";
            OleDbCommand myCommand;
            OleDbDataReader myDataReader;
            OleDbConnection myConnection;

            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT [CONTACT NUMBER] FROM [contacts] WHERE [ID] = @id";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@id", (id)));
                myConnection.Open();
                myDataReader = myCommand.ExecuteReader();

                while (myDataReader.Read())
                {
                    contactnumber = myDataReader["CONTACT NUMBER"].ToString();
                }
                myConnection.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            return contactnumber;
        }

        private void radioButton2_CheckedChanged(object sender, EventArgs e)
        {
            txtNumber.Enabled = false;
            textBox1.Enabled = false;
            textBox2.Enabled = false;
            comboBox1.Enabled = false;
            textBox2.Text = CountContacts().ToString();
        }

        private void radioButton3_CheckedChanged(object sender, EventArgs e)
        {
            textBox1.Enabled = true;
            textBox2.Enabled = true;
            txtNumber.Enabled = false;
            comboBox1.Enabled = false;
            textBox2.Text = CountContacts().ToString();
        }

        private void radioButton1_CheckedChanged(object sender, EventArgs e)
        {
            txtNumber.Enabled = true;
            textBox1.Enabled = false;
            textBox2.Enabled = false;
            comboBox1.Enabled = false;
            textBox2.Text = "1";
        }

        private void ShowMessage(SmsPdu pdu)
        {
            if (pdu is SmsSubmitPdu)
            {
                // Stored (sent/unsent) message
                SmsSubmitPdu data = (SmsSubmitPdu)pdu;
                OutputSent("SENT MESSAGE");
                OutputSent("Recipient: " + data.DestinationAddress);
                OutputSent("Message text: " + data.UserDataText);
                OutputSent("-------------------------------------------------------------------");
                return;
            }
            if (pdu is SmsDeliverPdu)
            {
                // Received message
                SmsDeliverPdu data = (SmsDeliverPdu)pdu;
                UpdateLog("Received", "Reply", data.OriginatingAddress, data.UserDataText);
                OutputReceived("RECEIVED MESSAGE");
                OutputReceived("Sender: " + data.OriginatingAddress);
                OutputReceived("Area: " + GetArea(data.OriginatingAddress.ToString()));
                OutputReceived("Sent: " + data.SCTimestamp.ToString());
                OutputReceived("Message text: " + data.UserDataText);
                OutputReceived("-------------------------------------------------------------------");

                OutputCPnumber(data.OriginatingAddress);
                varmessage = data.UserDataText;
                string[] message = varmessage.Split('/');

                if (message[0].ToUpper() == "AREA")
                {
                    if (message.Length == 2)
                    {
                        OutputAreaMobileNumber(message[1]);
                        OutputCode(message[0]);

                        return;
                    }
                    else
                    {
                        SendMessage("Invalid Keywords", cpnumberTxtBox.Text, "Sorry, You Texted An Invalid Command.");
                    }
                }
                else if (message[0].ToUpper() == "TB")
                {
                    if (message.Length == 3)
                    {
                        OutputGroup(message[1]);
                        OutputMessage(message[2]);
                        OutputCode(message[0]);

                        return;
                    }
                    else
                    {
                        SendMessage("Invalid Keywords", cpnumberTxtBox.Text, "Sorry, You Texted An Invalid Command. Text HELP For More Information.");
                    }
                }
                else if (message[0].ToUpper() == "TBALL")
                {
                    if (message.Length == 2)
                    {
                        OutputMessage(message[1]);
                        OutputCode(message[0]);

                        return;
                    }
                    else
                    {
                        SendMessage("Invalid Keywords", cpnumberTxtBox.Text, "Sorry, You Texted An Invalid Command. Text HELP For More Information.");
                    }
                }
            }
            else
            {
                Output("Unknown message type: " + pdu.GetType().ToString());
            }
        }

        public void SendMessage(string description, string mobilenumber, string textmessage)
        {
            Cursor.Current = Cursors.WaitCursor;
            SmsSubmitPdu pdu;
            try
            {
                pdu = new SmsSubmitPdu(textmessage, mobilenumber);
                comm.SendMessage(pdu);
                UpdateLog("Sent", description, mobilenumber, textmessage);
                Output("1 Message Sent To: " + mobilenumber);
                Output(" ");
                ShowMessage(pdu);
            }
            catch (Exception ex)
            {
                UpdateLog("Unsent", description, mobilenumber, textmessage);
                Output("1 Message Not Sent To: " + mobilenumber + ". Error: " + ex.Message);
                Output(" ");
            }
            Cursor.Current = Cursors.Default;
        }

        private void comm_MessageReceived(object sender, MessageReceivedEventArgs e)
        {
            try
            {
                IMessageIndicationObject obj = e.IndicationObject;
                if (obj is MemoryLocation)
                {
                    MemoryLocation loc = (MemoryLocation)obj;
                    Output(string.Format("New message received in storage \"{0}\", index {1}.",
                        loc.Storage, loc.Index));
                    Output("");
                    return;
                }
                if (obj is ShortMessage)
                {
                    ShortMessage msg = (ShortMessage)obj;
                    SmsPdu pdu = comm.DecodeReceivedMessage(msg);
                    Output("New message received.");
                    Output("");
                    ShowMessage(pdu);
                    return;
                }
                Output("Error: Unknown notification object!");
            }
            catch (Exception ex)
            {
                Output(ex.ToString());
            }
        }

        private void Form1_FormClosed(object sender, FormClosedEventArgs e)
        {
            Environment.Exit(0);
        }

        private void txtMessage_TextChanged(object sender, EventArgs e)
        {
            label6.Text = Convert.ToString(txtMessage.Text.Length);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            InboxForm inboxform = new InboxForm();
            inboxform.Show();
        }

        private void codeTxtBox_TextChanged(object sender, EventArgs e)
        {
            if (codeTxtBox.Text.ToUpper() == "AREA" & areacpnumberTxtBox.Text != "")
            {
                UpdateLog("Received", "Area Query", cpnumberTxtBox.Text, varmessage);
                SearchArea(areacpnumberTxtBox.Text);
            }
            else if (codeTxtBox.Text.ToUpper() == "TB" & groupTxtBox.Text != "" & txtMessage.Text != "")
            {
                    UpdateLog("Received", "Text Blast - Granted", cpnumberTxtBox.Text, varmessage);
                    SendToGroup(groupTxtBox.Text);
            }
            else if (codeTxtBox.Text.ToUpper() == "TBALL" & txtMessage.Text != "")
            {
                UpdateLog("Received", "Text Blast All - Granted", cpnumberTxtBox.Text, varmessage);
                timer1.Start();
            }
        }

        public string GetArea(string mobilenumber)
        {
            string MyConnectionString;
            string mySQLQuery;
            OleDbCommand myCommand;
            OleDbDataReader myDataReader;
            OleDbConnection myConnection;
            string group = "";
            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT * FROM [contacts] WHERE [CONTACT NUMBER] = @mobilenumber";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@mobilenumber", (mobilenumber)));
                myConnection.Open();
                myDataReader = myCommand.ExecuteReader();

                while (myDataReader.Read())
                {
                    group = myDataReader["GROUP"].ToString();
                }

                myConnection.Close();
                
                
                return group;
            }
            catch (Exception ex)
            {
                Output("Error: " + ex.ToString());
                return group;
            }
        }

        public void SearchArea(string mobilenumber)
        {
            string MyConnectionString;
            string mySQLQuery;
            OleDbCommand myCommand;
            OleDbDataReader myDataReader;
            OleDbConnection myConnection;
            string group = "";
            string tmpmobilenumber = "";

            if (mobilenumber.Substring(0, 1) == "0")
            {
                tmpmobilenumber = mobilenumber.Substring(1, mobilenumber.Length - 1);
            }
            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT * FROM [contacts] WHERE [CONTACT NUMBER] LIKE '%' & @mobilenumber";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@mobilenumber", (tmpmobilenumber)));
                myConnection.Open();
                myDataReader = myCommand.ExecuteReader();

                bool mobilenumberFound = false;
               
                
                while (myDataReader.Read())
                {
                    mobilenumberFound = true;
                    group = myDataReader["GROUP"].ToString();
                 }

                myConnection.Close();

                if (mobilenumberFound == true)
                {
                    SendMessage("Area Query - Found", cpnumberTxtBox.Text, "Mobile Number: " + mobilenumber + " Area: " + group);
                    Clear();
                }
                else
                {
                    SendMessage("Area Query - Not Found", cpnumberTxtBox.Text, "Mobile Number: " + mobilenumber + " - Not Yet Registered.");
                    Clear();
                }
            }
            catch (Exception ex)
            {
                Output("Error: " + ex.ToString());
            }
        }

        public void Clear()
        {
            cpnumberTxtBox.Clear();
            areacpnumberTxtBox.Clear();
            groupTxtBox.Clear();
            txtMessage.Clear();
            codeTxtBox.Clear();
        }

        private void GetGroups()
        {
            string MyConnectionString;
            string mySQLQuery;
            OleDbCommand myCommand;
            OleDbDataReader myDataReader;
            OleDbConnection myConnection;

            try
            {

                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT DISTINCT [GROUP] FROM [contacts] ORDER BY [GROUP]";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myConnection.Open();
                myDataReader = myCommand.ExecuteReader();

                while (myDataReader.Read())
                {
                    comboBox1.Items.Add(myDataReader["GROUP"].ToString().ToUpper());
                }

                myConnection.Close();
            }
            catch (Exception ex)
            {
                Output("Error: " + ex.ToString());
            }
        }

        private List<int> SendToGroup(string group)
        {
            string MyConnectionString;
            string mySQLQuery;
            OleDbCommand myCommand;
            OleDbDataReader myDataReader;
            OleDbConnection myConnection;
            List<int> ids = new List<int>();
            try
            {

                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT [ID] FROM [contacts] WHERE [GROUP] = @group";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@group", (group)));
                myConnection.Open();
                myDataReader = myCommand.ExecuteReader();
               
                while (myDataReader.Read())
                {
                   /* if (txtMessage.Text.Length <= 160)
                    {
                        SendMessage("Text Blast Message", myDataReader["CONTACT NUMBER"].ToString(), txtMessage.Text);
                    }
                    else if (txtMessage.Text.Length > 160)
                    {
                        OutgoingSmsPdu[] pdus = CreateConcatMessage(txtMessage.Text, myDataReader["CONTACT NUMBER"].ToString());
                        if (pdus != null)
                        {
                            SendMultiple(pdus, myDataReader["CONTACT NUMBER"].ToString(), "Text Blast Message");
                        }
                    }*/
                    ids.Add(Convert.ToInt32(myDataReader["ID"]));
                }
                myConnection.Close();
                return ids;
            }
            catch (Exception ex)
            {
                Output("Error: " + ex.ToString());
                return ids;
            }
        }

        private void radioButton4_CheckedChanged(object sender, EventArgs e)
        {
            txtNumber.Enabled = false;
            textBox1.Enabled = false;
            textBox2.Enabled = false;
            comboBox1.Enabled = true;
            comboBox1.DropDownStyle = ComboBoxStyle.DropDownList;
            GetGroups();
        
        }

        private void button2_Click(object sender, EventArgs e)
        {
            ConvertContacts();
        }

        public void ConvertContacts()
        {
            string MyConnectionString;
            string mySQLQuery = "";
            OleDbCommand myCommand;
            OleDbDataReader myDataReader;
            OleDbConnection myConnection;

            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "SELECT * FROM [contacts]";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myConnection.Open();
                myDataReader = myCommand.ExecuteReader();

                while (myDataReader.Read())
                {
                    string contactnumber = myDataReader["CONTACT NUMBER"].ToString();
                    string id = myDataReader["ID"].ToString();

                    if (contactnumber.Substring(0, 1) == "0")
                    {
                        contactnumber = "+63" + contactnumber.Substring(1, contactnumber.Length - 1);
                        UpdateContacts(id, contactnumber);
                    }
                }

                myConnection.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        public void UpdateContacts(string id, string number)
        {
            string MyConnectionString;
            string mySQLQuery = "";
            OleDbCommand myCommand;
            OleDbConnection myConnection;
            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "UPDATE [contacts] SET [CONTACT NUMBER] = @number WHERE [ID] = @id";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@number", (number)));
                myCommand.Parameters.Add(new OleDbParameter("@id", (id)));
                myConnection.Open();
                myCommand.ExecuteNonQuery();
                myConnection.Close();
                Output("1 Contact Number Converted To: " + number);
                Output("");
            }
            catch (Exception)
            {
                DeleteContact(id);
                Output("1 Contact Deleted Because of Duplicate");
                Output("");
            }
        }

        public void DeleteContact(string id)
        {
            string MyConnectionString;
            string mySQLQuery = "";
            OleDbCommand myCommand;
            OleDbConnection myConnection;
            try
            {
                MyConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=Database2.mdb;";
                myConnection = new OleDbConnection(MyConnectionString);
                mySQLQuery = "DELETE FROM [contacts] WHERE [ID] = @id";
                myCommand = new OleDbCommand(mySQLQuery, myConnection);
                myCommand.Parameters.Add(new OleDbParameter("@id", (id)));
                myConnection.Open();
                myCommand.ExecuteNonQuery();
                myConnection.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error: " + id + " " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }


    }
}