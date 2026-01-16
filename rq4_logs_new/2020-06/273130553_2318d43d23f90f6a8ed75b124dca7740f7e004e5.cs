using Eproduction.Integrated.Assistant.Common.Core;
using Eproduction.Integrated.Assistant.Common.Worker;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace Eproduction.Integrated.Assistant.Common.Control {
    public partial class SqlServerDbSelector : UserControl {
        private string _connectionString = string.Empty;
        private SqlServerDbContext _dataSource = null;
        public SqlServerDbContext DataSource {
            get {
                if (_dataSource == null) {
                    _dataSource = new SqlServerDbContext();
                }
                return _dataSource;
            }
            set => _dataSource = value;
        }
        public string ConnectionString {
            get => _connectionString;
            set => _connectionString = value;
        }
        private AvailableSQLServersFillProxy _availableSQLServersFillProxy = null;
        private delegate void AvailableSQLServersFillProxy(ComboBox cbbox, IEnumerable<string> namelist);
        private SelectDatabaseProxy _selectDatabaseProxy = null;
        private delegate void SelectDatabaseProxy(ComboBox cbbox, int selectedIndex);
        private SetComboItemsProxy _setComboItemsProxy = null;
        private delegate void SetComboItemsProxy(ComboBox cbbox, IEnumerable<object> items);
        private SetBindingProxy _setBindingProxy = null;
        private delegate void SetBindingProxy(System.Windows.Forms.Control component, string propertyName, BindingSource bindingSource, string dataMember);
        private ClearBindingsProxy _clearBindingsProxy = null;
        private delegate void ClearBindingsProxy(System.Windows.Forms.Control component);
        public SqlServerDbSelector() {
            _availableSQLServersFillProxy = new AvailableSQLServersFillProxy(delegate (ComboBox cbbox, IEnumerable<string> namelist) {
                if (cbbox.Items.Count > 0) cbbox.Items.Clear();
                cbbox.Items.AddRange(namelist.ToArray());
                cbbox.SelectedIndex = 0;
            });
            _selectDatabaseProxy = new SelectDatabaseProxy(delegate (ComboBox cbbox, int selectedIndex) {
                if (cbbox.Items.Count > 0 && selectedIndex >= 0 && selectedIndex < cbbox.Items.Count) {
                    cbbox.SelectedIndex = selectedIndex;
                }
            });
            _setComboItemsProxy = new SetComboItemsProxy(delegate (ComboBox cbbox, IEnumerable<object> items) {
                if (cbbox.Items.Count > 0) cbbox.Items.Clear();
                cbbox.Items.AddRange(items.ToArray());
            });
            _setBindingProxy = new SetBindingProxy(delegate (System.Windows.Forms.Control component, string propertyName, BindingSource bindingSource, string dataMember) {
                component.DataBindings.Add(new Binding(propertyName, bindingSource, dataMember, false, DataSourceUpdateMode.OnPropertyChanged));
            });
            _clearBindingsProxy = new ClearBindingsProxy(delegate (System.Windows.Forms.Control component) {
                if (component.DataBindings.Count > 0) component.DataBindings.Clear();
            });

            InitializeComponent();

            EventHandler TextChangedEventHandler = delegate (object o, EventArgs v) {
                var sender = (TextBox)o;
                if (string.IsNullOrEmpty(sender.Text)) {
                    SetErrorProvider(sender, "[不可空白]");
                } else {
                    SetErrorProvider(sender, "");
                }
            };
            EventHandler ComboBoxTextChangedEventHandler = delegate (object o, EventArgs e) {
                var sender = (ComboBox)o;
                if (string.IsNullOrEmpty(sender.Text)) {
                    SetErrorProvider(sender, "[不可空白]");
                } else {
                    SetErrorProvider(sender, "");
                }
            };
            cbboxServer.TextChanged += ComboBoxTextChangedEventHandler;
            cbboxIdentityVerifyType.SelectedIndexChanged += (o, v) => {
                if (DataSource.IdentityVerifyType == IdentityType.Windows) {
                    SetErrorProvider(tboxUsername, "");
                    SetErrorProvider(tboxPassport, "");
                }
            };
            tboxUsername.TextChanged += TextChangedEventHandler;
            tboxPassport.TextChanged += TextChangedEventHandler;
            cbboxChooseDatabase.TextChanged += ComboBoxTextChangedEventHandler;

            Dock = DockStyle.Fill;
        }
        private void SqlServerDbSelector_Load(object sender, EventArgs e) {
            try {
                SetBindings(false);
                //if (InstallContext.InspectParameters.IsInstallSqlServer) {
                //    //为数据库服务器下来列表绑定数据源
                //    Invoke(_availableSQLServersFillProxy, new object[] { cbboxServer, SqlServerWorker.GetAvailableSQLServers() });

                //    if (!string.IsNullOrEmpty(DataSource.Server) && DataSource.IdentityVerifyType == IdentityType.Windows) {
                //        FillDatabaseList();
                //    }
                //}
            } catch (Exception) { throw; }
        }
        public void SetBindings(bool rebinding) {
            Reload();
            BindingSource.DataSource = DataSource;

            if (rebinding) {
                Invoke(_clearBindingsProxy, new object[] { cbboxServer });
                Invoke(_clearBindingsProxy, new object[] { cbboxIdentityVerifyType });
                Invoke(_clearBindingsProxy, new object[] { tboxUsername });
                Invoke(_clearBindingsProxy, new object[] { tboxPassport });
                Invoke(_clearBindingsProxy, new object[] { cbboxChooseDatabase });
            }
            Invoke(_setBindingProxy, new object[] { cbboxServer, "Text", BindingSource, "Server" });
            Invoke(_setBindingProxy, new object[] { cbboxIdentityVerifyType, "SelectedIndex", BindingSource, "IdentityVerify" });
            Invoke(_setBindingProxy, new object[] { tboxUsername, "Text", BindingSource, "Username" });
            Invoke(_setBindingProxy, new object[] { tboxPassport, "Text", BindingSource, "Passport" });
            Invoke(_setBindingProxy, new object[] { cbboxChooseDatabase, "Text", BindingSource, "Catalog" });
        }
        public void Reload() {
            DataSource.Analyze(ConnectionString);
        }
        public void SelectDatabase(int selectedIndex) {
            Invoke(_selectDatabaseProxy, new object[] { cbboxChooseDatabase, selectedIndex });
        }
        private void SetErrorProvider(System.Windows.Forms.Control control, string message) {

        }
        private void cbboxIdentityVerifyType_SelectedIndexChanged(object sender, EventArgs e) {
            var comboBox = (ComboBox)sender;
            tboxUsername.ReadOnly = tboxPassport.ReadOnly = ((IdentityType)comboBox.SelectedIndex) == IdentityType.Windows;
        }
        private void cbboxChooseDatabase_DropDown(object sender, EventArgs e) {
            FillDatabaseList();
        }
        private void btnTestConnect_Click(object sender, EventArgs e) {
            if (!DataSource.VerifyWithTestConnect()) return;

            try {
                if (SqlServerWorker.GetDatabaseList(DataSource.MasterDbConnectionString).Contains(DataSource.Catalog)) {
                    if (SqlServerWorker.TestExecute(DataSource.ConnectionString) != null) {
                        MessageBox.Show("测试成功！", "提示", MessageBoxButtons.OK, MessageBoxIcon.Information, MessageBoxDefaultButton.Button1);
                    }
                } else {
                    MessageBox.Show("未能找到数据库 " + DataSource.Catalog, "错误", MessageBoxButtons.OK, MessageBoxIcon.Error, MessageBoxDefaultButton.Button1);
                }
            } catch (Exception ex) {
                MessageBox.Show(ex.Message, "程序发生异常", MessageBoxButtons.OK, MessageBoxIcon.Error, MessageBoxDefaultButton.Button1);
            }
        }
        public void FillDatabaseList() {
            if (!DataSource.VerifyWithGetDbList()) return;

            try {
                if (string.IsNullOrEmpty(DataSource.Server) || (DataSource.IdentityVerifyType == IdentityType.SQLServer && (string.IsNullOrEmpty(DataSource.Username) || string.IsNullOrEmpty(DataSource.Passport)))) {
                    return;
                }
                
                var databaseList = SqlServerWorker.GetDatabaseList(DataSource.MasterDbConnectionString);
                Invoke(_setComboItemsProxy, new object[] { cbboxChooseDatabase, databaseList });
            } catch (Exception ex) {
                MessageBox.Show(ex.Message, "程序发生异常", MessageBoxButtons.OK, MessageBoxIcon.Error, MessageBoxDefaultButton.Button1);
            }
        }
        public bool Validator(bool catalogMustExists) {
            bool successfully = true;

            string message = string.Empty;
            if (string.IsNullOrEmpty(DataSource.Server)) {
                successfully = false;
                message = "[不可空白]";
            } else {
                message = string.Empty;
            }
            SetErrorProvider(cbboxServer, message);
            if (string.IsNullOrEmpty(DataSource.Catalog)) {
                successfully = false;
                message = "[不可空白]";
            } else {
                message = string.Empty;
            }
            SetErrorProvider(cbboxChooseDatabase, message);
            if (DataSource.IdentityVerifyType == IdentityType.SQLServer) {
                if (string.IsNullOrEmpty(DataSource.Username)) {
                    successfully = false;
                    message = "[不可空白]";
                } else {
                    message = string.Empty;
                }
                SetErrorProvider(tboxUsername, message);
                if (string.IsNullOrEmpty(DataSource.Passport)) {
                    successfully = false;
                    message = "[不可空白]";
                } else {
                    message = string.Empty;
                }
                SetErrorProvider(tboxPassport, message);
            }
            if (successfully) {
                try {
                    DataSource.Exists = SqlServerWorker.GetDatabaseList(DataSource.MasterDbConnectionString).Contains(DataSource.Catalog);
                    if (DataSource.Exists) {
                        successfully = SqlServerWorker.TestExecute(DataSource.ConnectionString) != null;
                    } else {
                        if (catalogMustExists) {
                            successfully = false;
                            MessageBox.Show("远程服务器未能找到数据库 " + DataSource.Catalog, "错误", MessageBoxButtons.OK, MessageBoxIcon.Error, MessageBoxDefaultButton.Button1);
                        }
                    }
                } catch (Exception) {
                    throw;
                }
            }

            return successfully;
        }
    }
}