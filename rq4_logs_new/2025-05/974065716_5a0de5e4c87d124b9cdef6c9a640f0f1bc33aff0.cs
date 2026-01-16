using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.ComponentModel;

namespace EasySave.Views {
    /// <summary>
    /// Interaction logic for SelectProcess.xaml
    /// </summary>
    public partial class SelectProcess : Window {
        public class ProcessInfo {
            public string Name { get; set; }
            public int Id { get; set; }
            public string MemoryMB { get; set; }
            public string Path { get; set; }
        }

        public string? SelectedProcessName { get; private set; }

        private List<ProcessInfo> _allProcesses = new();

        public SelectProcess() {
            InitializeComponent();
            LoadProcesses();
            ProcessListView.SelectionChanged += ProcessListView_SelectionChanged;
            OkButton.IsEnabled = false;
        }

        private void LoadProcesses() {
            var processes = Process.GetProcesses()
                .OrderBy(p => p.ProcessName)
                .Select(p => {
                    string path = "";
                    try { path = p.MainModule?.FileName ?? ""; } catch { }
                    string mem = "";
                    try { mem = (p.WorkingSet64 / (1024 * 1024)).ToString(); } catch { }
                    return new ProcessInfo {
                        Name = p.ProcessName,
                        Id = p.Id,
                        MemoryMB = mem,
                        Path = path
                    };
                })
                .GroupBy(p => p.Name)
                .Select(g => g.First()) // Only show one entry per process name
                .ToList();

            _allProcesses = processes;
            ProcessListView.ItemsSource = _allProcesses;
        }

        private void SearchBox_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e) {
            string query = SearchBox.Text?.Trim() ?? "";
            if (string.IsNullOrEmpty(query)) {
                ProcessListView.ItemsSource = _allProcesses;
            } else {
                var filtered = _allProcesses.Where(p =>
                    (!string.IsNullOrEmpty(p.Name) && p.Name.Contains(query, StringComparison.OrdinalIgnoreCase)) ||
                    p.Id.ToString().Contains(query) ||
                    (!string.IsNullOrEmpty(p.Path) && p.Path.Contains(query, StringComparison.OrdinalIgnoreCase))
                ).ToList();
                ProcessListView.ItemsSource = filtered;
            }
        }

        private void ProcessListView_SelectionChanged(object sender, System.Windows.Controls.SelectionChangedEventArgs e) {
            OkButton.IsEnabled = ProcessListView.SelectedItem != null;
        }

        private void OkButton_Click(object sender, RoutedEventArgs e) {
            if (ProcessListView.SelectedItem is ProcessInfo info) {
                SelectedProcessName = info.Name;
                DialogResult = true;
                Close();
            }
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e) {
            DialogResult = false;
            Close();
        }
    }
}