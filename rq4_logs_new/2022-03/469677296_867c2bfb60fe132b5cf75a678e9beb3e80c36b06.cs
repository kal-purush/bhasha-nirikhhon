using DevExpress.Mvvm;
using ReportGenerator.DataBase.Models;
using ReportGenerator.Services;
using System.Windows;
using System.Windows.Input;

namespace ReportGenerator.ViewModels
{
    public class RoleEditWindowViewModel : BindableBase
    {
        public string Title { get; set; }
        public Role CurrentRole{ get; set; }

        public RoleEditWindowViewModel()
        {
            Title = "Добавление роли";
            MessageService.Bus += Receive;
        }

        private void Receive(object dataReceive)
        {
            if (dataReceive is Role data)
            {
                CurrentRole = data;
                Title = "Редактирование роли";
                MessageService.Bus -= Receive;
            }

            if (dataReceive is int index)
            {
                CurrentRole = new Role(index, "");
                MessageService.Bus -= Receive;
            }
        }

        public ICommand SendDialogResultRole => new DelegateCommand<object>((currentWindow) =>
        {
            if (!string.IsNullOrWhiteSpace(CurrentRole.name))
            {
                try
                {
                    Role _resultRole = new Role(CurrentRole.id, CurrentRole.name);
                    MessageService.Send(_resultRole);
                    Window wnd = currentWindow as Window;
                    wnd.DialogResult = true;
                    wnd.Close();
                }
                catch
                {
                    MessageBox.Show("Поля заполнены не корректно!", "Ошибка");
                }
            }

            else
            {
                MessageBox.Show("Не все поля заполнены!", "Ошибка");
            }
        });
    }
}