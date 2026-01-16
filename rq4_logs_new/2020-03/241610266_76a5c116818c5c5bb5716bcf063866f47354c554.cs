using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

namespace Employees
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        ObservableCollection<Employee> Employees = new ObservableCollection<Employee>();
        ObservableCollection<Department> Departments = new ObservableCollection<Department>();

        public MainWindow()
        {
            InitializeComponent();
            FillList();
        }

        void FillList()
        {
            // Заполняем департаменты
            Departments.Add(new Department() { Name = "Сисадмины", Manager = null });
            Departments.Add(new Department() { Name = "Разработчики", Manager = null });
            Departments.Add(new Department() { Name = "Архитекторы", Manager = null });

            // Ищем департамент "Programmers" для добавления в него сотрудников
            Department ProgDep = null;
            foreach(var dep in Departments)
                if(dep.Name == "Разработчики")
                {
                    ProgDep = dep;
                    break;
                }

            // добавляем сотрудников
            // менеджер
            var ProgManager = new Employee() { Name = "Василий", Age = 22, Salary = 3000, Dep = ProgDep };
            Employees.Add(ProgManager);
            // еще сотрудники
            Employees.Add(new Employee() { Name = "Петр", Age = 25, Salary = 6000, Dep = ProgDep });
            Employees.Add(new Employee() { Name = "Николай", Age = 23, Salary = 8000, Dep = ProgDep });

            // назначаем менеджера департаменту
            ProgDep.Manager = ProgManager;

            // связываем элемент ListBox с коллекцией сотрудников
            lbEmployee.ItemsSource = Employees;
        }

        private void btnAdd_Click(object sender, RoutedEventArgs e)
        {
            // === временно левые данные ===
            // Ищем департамент "Programmers" для добавления в него сотрудников
            Department ProgDep = null;
            foreach (var dep in Departments)
                if (dep.Name == "Разработчики")
                {
                    ProgDep = dep;
                    break;
                }

            Employees.Add(new Employee() {Name = "Сергей", Age = 26, Salary = 7000, Dep = ProgDep});
        }

        private void btnDelete_Click(object sender, RoutedEventArgs e)
        {
            // удаление выделенного сотрудника
            Employees.Remove((Employee)lbEmployee.SelectedItem);
        }
    }

    public static class GID  // Независимая генерация Id
    {
        private static int _EmpId = 0;
        private static int _DepId = 0;

        public static int GetEmpId()
        {
            _EmpId++;
            return _EmpId;
        }

        public static int GetDepId()
        {
            _DepId++;
            return _DepId;
        }
    }

    public class Employee
    {
        public int Id { get; private set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public double Salary { get; set; }
        public Department Dep { get; set; }

        public Employee()
        {
            Id = GID.GetEmpId();
        }

        public override string ToString()
        {
            string IsManager = (Id == Dep?.Manager?.Id) ? " (начальник)" : "";
            return $"{Id}\t{Name}\t{Age}\t{Salary}\t{Dep.Name}{IsManager}";
        }
    }

    public class Department
    {
        public int Id { get; private set; }
        public string Name { get; set; }
        public Employee Manager { get; set; }

        public Department()
        {
            Id = GID.GetDepId();
        }
    }
}