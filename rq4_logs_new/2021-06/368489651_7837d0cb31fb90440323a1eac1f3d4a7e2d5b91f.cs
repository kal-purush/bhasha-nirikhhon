using ExpensesTracker.Methods;
using ExpensesTracker.Models;
using ExpensesTracker.Models.Enums;
using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Windows.Forms;

namespace ExpensesTracker.Forms
{
    public partial class MainWindow : Form
    {
        private Account TargetAccount => (Account)selectedAccountBox.SelectedItem;
        private List<AccountOperation> _filteredOperations = new List<AccountOperation>();
        private List<Account> _accounts = new List<Account>();
        private List<AccountOperation> _accountOperations = new List<AccountOperation>();
        private readonly PDFGenerator _pdfGenerator = new PDFGenerator();
        private readonly Storage _storage = new Storage();

        public MainWindow()
        {
            InitializeComponent();
        }

        private void MainWindow_Load(object sender, EventArgs e)
        {
            _storage.LoadData();
            _accounts = _storage.Accounts;
            _accountOperations = _storage.AccountOperations;
            selectedAccountBox.DataSource = null;
            selectedAccountBox.DataSource = _accounts;
            selectedAccountBox.DisplayMember = "Name";
            selectedAccountBox.SelectedIndex = 0;
            expensesTable.AutoGenerateColumns = false;
            categoryFilterBox.SelectedItem = "All amount";
        }

        private void CategoryFilter_SelectedIndexChanged(object sender, EventArgs e)
        {
            FilterTable();
        }

        private void RefreshData()
        {
            _storage.SaveChanges(_accounts, _accountOperations);
            RefreshTable();
            FilterTable();
            ShowBalance();
        }

        private void CommentSearch_TextChanged(object sender, EventArgs e)
        {
            FilterTable();
        }

        public void RefreshTable()
        {
            expensesTable.DataSource = null;
            expensesTable.DataSource = _filteredOperations;
            expensesTable.Columns[4].Visible = false;
            expensesTable.Columns[5].Visible = false;
        }

        private void DateTimePicker1_ValueChanged(object sender, EventArgs e)
        {
            FilterTable();
        }

        private void DateTimePicker2_ValueChanged(object sender, EventArgs e)
        {
            FilterTable();
        }

        private void TableColoring()
        {
            for (int i = 0; i < _filteredOperations.Count; i++)
            {
                if (_filteredOperations[i].Type == OperationType.Expense)
                {
                    expensesTable.Rows[i].DefaultCellStyle.BackColor = Color.Red;
                }
                if (_filteredOperations[i].Type == OperationType.Income)
                {
                    expensesTable.Rows[i].DefaultCellStyle.BackColor = Color.Green;
                }
            }
        }

        private void FilterTable()
        {
            _filteredOperations = new List<AccountOperation>();
            _filteredOperations = _accountOperations.Where(x => x.Account.Name == TargetAccount.Name).ToList();
            _filteredOperations.Sort((x, y) => x.Date.CompareTo(y.Date));

            //category Filtred
            var chosenCategory = Convert.ToString(categoryFilterBox.SelectedItem);

            if (chosenCategory != "All amount")
            {
                _filteredOperations = _filteredOperations.Where(x => Convert.ToString(x.Category) == chosenCategory).ToList();
            }

            //name search
            if (!string.IsNullOrWhiteSpace(searchNameInput.Text))
            {
                _filteredOperations = _filteredOperations.Where(x => x.Comment.Contains(searchNameInput.Text.Trim())).ToList();
            }

            //data filrer1
            _filteredOperations = _filteredOperations.Where(x => x.Date >= startDateDisplay.Value).ToList();

            //data filter2
            _filteredOperations = _filteredOperations.Where(x => x.Date <= endDateDisplay.Value).ToList();
            RefreshTable();
            TableColoring();
        }

        private void SavePdfButton_Click(object sender, EventArgs e)
        {
            _pdfGenerator.GeneratePdf(_filteredOperations);
        }

        private void ExpensesTable_CellContentDoubleClick(object sender, DataGridViewCellEventArgs e)
        {
            EditOperation();
        }

        private void EditExpensButton_Click(object sender, EventArgs e)
        {
            EditOperation();
        }

        private void EditOperation()
        {
            var operation = expensesTable.SelectedRows[0].DataBoundItem as AccountOperation;
            if (operation.Type == OperationType.Expense)
            {
                EditDataFormForExpense editDataForm = new EditDataFormForExpense();
                editDataForm.TargetExpense = operation;
                editDataForm.OnExpenseEdit = () =>
                {
                    RefreshData();
                };
                editDataForm.OnExpenseDeleted = () =>
                {
                    if (operation != null)
                    {
                        _accountOperations.Remove(operation);
                    }
                    RefreshData();
                };
                editDataForm.Show();
            }
            else
            {
                EditDataFormForIncome editIncomeForm = new EditDataFormForIncome();
                editIncomeForm.TargetIncome = operation;
                editIncomeForm.OnIncomeEdit = () =>
                {
                    RefreshData();
                };
                editIncomeForm.OnIncomeDeleted = () =>
                {
                    if (operation != null)
                    {
                        _accountOperations.Remove(operation);
                    }
                    RefreshData();
                };
                editIncomeForm.Show();
            }
        }

        private void RefreshAccount()
        {
            selectedAccountBox.DataSource = null;
            selectedAccountBox.DataSource = _accounts;
            selectedAccountBox.DisplayMember = "Name";
            selectedAccountBox.SelectedItem = TargetAccount;
        }

        public void SelectedAccountBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (selectedAccountBox.SelectedIndex == -1)
                selectedAccountBox.SelectedIndex = 0; /*TODO: Fix it later*/
            FilterTable();
            ShowBalance();
        }

        public void ShowBalance()
        {
            var expensesSum = _accountOperations.Where(x => x.Account.Name == TargetAccount.Name && x.Type == OperationType.Expense).Sum(x => x.Amount);
            var incomesSum = _accountOperations.Where(x => x.Account.Name == TargetAccount.Name && x.Type == OperationType.Income).Sum(x => x.Amount);
            accountBalance.Text = Convert.ToString(TargetAccount.InitialBalance - expensesSum + incomesSum);
        }

        private void AddAccountForm_Click(object sender, EventArgs e)
        {
            AddNewAccount addAccountForm = new AddNewAccount();
            addAccountForm.OnAccountAdded = (account) =>
            {
                _accounts.Add(account);
                RefreshAccount();
                _storage.SaveChanges(_accounts, _accountOperations);
            };
            addAccountForm.Show();
        }

        private void EditAccountButton_Click(object sender, EventArgs e)
        {
            Account account = (Account)selectedAccountBox.SelectedItem;
            EditAccount editAccountForm = new EditAccount();
            editAccountForm.TargetAccountForEdit = TargetAccount;
            editAccountForm.OnAccountEdit = () =>
            {
                RefreshAccount();
                ShowBalance();
                _storage.SaveChanges(_accounts, _accountOperations);
            };
            editAccountForm.OnAccountDeleted = () =>
            {
                if (TargetAccount != null)
                {
                    _accounts.Remove(TargetAccount);
                    selectedAccountBox.SelectedIndex = 0;
                    RefreshData();
                    RefreshAccount();
                }
            };
            editAccountForm.Show();
        }

        private void AddIncomeForm_Click(object sender, EventArgs e)
        {
            AddNewIncomeForm addNewIncomeForm = new AddNewIncomeForm();
            addNewIncomeForm.OnIncomeAdded = (income) =>
            {
                income.Account = TargetAccount;
                _accountOperations.Add(income);
                RefreshData();
            };
            addNewIncomeForm.Show();
        }
        private void AddExpenseForm_Click(object sender, EventArgs e)
        {
            AddExpenseForm addCostForm = new AddExpenseForm();
            addCostForm.OnExpenseAdded = (expense) =>
            {
                expense.Account = TargetAccount;
                _accountOperations.Add(expense);
                RefreshData();
            };
            addCostForm.Show();
        }
    }
}