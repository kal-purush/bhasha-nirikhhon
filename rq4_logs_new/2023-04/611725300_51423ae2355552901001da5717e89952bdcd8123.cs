using System;
using System.Collections.Generic;
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
using System.Windows.Forms;
using System.IO;
using Newtonsoft.Json;

namespace Project
{
    /// <summary>
    /// Interaction logic for FileWritePage.xaml
    /// </summary>
    public partial class FileWritePage : Page, IFileWritePage
    {
        public void NewFileWritePage()
        {
            if (null == Global.Window)
            {
                return;
            }
            InitializeComponent();

            this.DataContext = this;
            Global.Window.Content = this;
        }

        public int TaskCount { get { return Global.Tasks.Count; } }

        private void TxtExport(object sender, RoutedEventArgs e)
        {
            var dialog = new FolderBrowserDialog();
            DialogResult result = dialog.ShowDialog();

            if(result == DialogResult.OK)
            {
                string path = dialog.SelectedPath + "\\SCRUM_Session_" + DateTime.Now.ToString("dd/MM/yyyy") + ".txt";
                StreamWriter writer = new StreamWriter(path);

                writer.WriteLine(DateTime.Now.ToString());
                writer.WriteLine();
                writer.WriteLine("Project: " + Global.ProjectName);
                writer.WriteLine("SCRUM master: " + Global.ScrumMasterName);
                writer.WriteLine("==================================");

                foreach (Task task in Global.Tasks)
                {
                    writer.WriteLine(task.Name);
                    writer.WriteLine(task.Description);
                    writer.WriteLine("Story points: " + task.Rating);
                    writer.WriteLine("==================================");
                }

                writer.WriteLine("Generated " + Global.Tasks.Count + " tasks");
                writer.Close();
            }
        }

        private void JsonExport(object sender, RoutedEventArgs e)
        {

            var dialog = new FolderBrowserDialog();
            DialogResult result = dialog.ShowDialog();

            if (result == DialogResult.OK)
            {
                string path = dialog.SelectedPath + "\\SCRUM_Session_" + DateTime.Now.ToString("dd/MM/yyyy") + ".json";
                StreamWriter writer = new StreamWriter(path);

                JsonOutput json = new JsonOutput
                {
                    ProjectName = Global.ProjectName,
                    ScrumMasterName = Global.ScrumMasterName,
                    CurrentDate = DateTime.Today,
                    TaskCount = Global.Tasks.Count,
                    Tasks = Global.Tasks
                };

                writer.WriteLine(JsonConvert.SerializeObject(json, Formatting.Indented));

                writer.Close();
            }
        }

        private void ExitPoker(object sender, RoutedEventArgs e)
        {
            System.Windows.Application.Current.Shutdown();
        }

    }

    internal class JsonOutput
    {
        private string projectName = "";
        private string scrumMasterName = "";
        private DateTime currentDate;
        private int taskCount;
        private List<Task> tasks = new List<Task>();

        public string ProjectName { get { return projectName; } set { projectName = value; } }
        public string ScrumMasterName { get { return scrumMasterName; } set {  scrumMasterName = value; } }
        public DateTime CurrentDate { get {  return currentDate; } set {  currentDate = value; } }
        public int TaskCount { get {  return taskCount; } set {  taskCount = value; } }
        public List<Task> Tasks { get {  return tasks; } set { tasks = value; } }
    }
}