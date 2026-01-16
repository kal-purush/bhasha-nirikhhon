using DevExpress.Mvvm;
using ReportGenerator.DataBase.Controls;
using ReportGenerator.DataBase.Models;
using ReportGenerator.Services;
using ReportGenerator.Views;
using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Input;

namespace ReportGenerator.ViewModels
{
    /// <summary>
    /// Вспомогательный клас для конвертации полей с ID в названия полей. Будет упразнен после написания конвертора.
    /// </summary>
    public class ItemUser
    {
        public int Id { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public DateTime Сreate_Date { get; set; }
        public string FullName { get; set; }
        public string Email { get; set; }        
        public string Departament { get; set; }        
        public string Role { get; set; }        
        public string Sector { get; set; }        
        public string Group { get; set; }

        public ItemUser()
        {
        }

            public ItemUser(User user)
        {
            Id = user.id;
            Username = user.username;
            Password = user.password;
            Сreate_Date = user.create_date;
            FullName = user.fullName;
            Email = user.email;
            Departament = DepartamentControl.GetNameById(user.departamentId);
            Role = RoleControl.GetNameById(user.roleId);
            Sector = SectorControl.GetNameById(user.sectorId);
            Group = GroupControl.GetNameById(user.groupId);
        }
    }

    /// <summary>
    /// Класс ViewModel для страницы настроек и управления
    /// </summary>
    public class AppManagerPageViewModel : BindableBase
    {
        public List<Departament> ListDepartaments { get; set; }
        private Departament _departamentSelected;
        public Departament DepartamentSelected
        {
            get { return _departamentSelected; }
            set
            {
                _departamentSelected = value;                
            }
        }
        public List<User> ListOfUsers { get; set; }
        public List<ItemUser> Users { get; set; }

        private User _newUser;
        private Departament _newDepartament;

        private SessionUser _sessionUser;

        private ItemUser _userSelected;
        public ItemUser UserSelected
        {
            get { return _userSelected; }
            set
            {
                _userSelected = value;               
            }
        }

        public AppManagerPageViewModel()
        {
            ListOfUsers = UserControl.GetAllUsersList();
            Users = new List<ItemUser>();
            foreach(User user in ListOfUsers)
            {
                Users.Add(new ItemUser (user));
            }
            
            ListDepartaments = new List<Departament>();
            ListDepartaments = DepartamentControl.GetAllDepartamentsList();
            MessageService.Bus += Receive;
        }

        /// <summary>
        /// Принимает объекты, необходимые для работы класса.
        /// SessionUser - пользователь текущей сесии
        /// User - пользователь из редактора (окна) пользователей
        /// Departament - отдел из редактора (окна) отделов
        /// </summary>        
        private void Receive(object data)
        {
            if (data is SessionUser sessionUser)
            {
                _sessionUser = sessionUser;
                MessageService.Bus -= Receive;
            }
            if (data is User user)
            {
                _newUser = user;
                MessageService.Bus -= Receive;
            }
            if (data is Departament departament)
            {
                _newDepartament = departament;
                MessageService.Bus -= Receive;
            }
        }
        
        /// <summary>
        /// Команда для открытия окна редактирования выбранного пользователя
        /// </summary>
        public ICommand OpenEditSelectedUserWindow => new DelegateCommand(() =>
        {
            User targetUser = new User(_userSelected.Id, _userSelected.Username, _userSelected.Password, _userSelected.Сreate_Date, _userSelected.FullName, _userSelected.Email, 
                DepartamentControl.GetIddByName(_userSelected.Departament), RoleControl.GetIddByName(_userSelected.Role), SectorControl.GetIddByName(_userSelected.Sector), GroupControl.GetIddByName(_userSelected.Group));
            
            UserEditWindow userEditWindow = new UserEditWindow();
            MessageService.Send(targetUser);
            MessageService.Bus += Receive;

            if (userEditWindow.ShowDialog() == true)
            {
                if (_newUser.id != 0)
                {
                    UserControl.UpdateCurrentUser(_newUser);
                    ListOfUsers = new List<User>();
                    ListOfUsers = UserControl.GetAllUsersList();
                    Users = new List<ItemUser>();
                    foreach (User user in ListOfUsers)
                    {
                        Users.Add(new ItemUser(user));
                    } 
                }
            }
            else
            {
                MessageService.Bus -= Receive;
            }
        }, () => _userSelected != null);

        /// <summary>
        /// Команда для открытия окна создания нового пользователя
        /// </summary>
        public ICommand OpenCreateNewUserWindow => new DelegateCommand(() =>
        {            
            UserEditWindow userEditWindow = new UserEditWindow();
            MessageService.Send(0);
            MessageService.Bus += Receive;

            if (userEditWindow.ShowDialog() == true)
            {
                if (_newUser.id == 0)
                {
                    UserControl.InsertNewUser(_newUser);
                    ListOfUsers = new List<User>();
                    ListOfUsers = UserControl.GetAllUsersList();
                    Users = new List<ItemUser>();
                    foreach (User user in ListOfUsers)
                    {
                        Users.Add(new ItemUser(user));
                    } 
                }
            }
            else
            {
                 MessageService.Bus -= Receive;
            }
        });

        /// <summary>
        /// Команда для удаления выбранного пользователя
        /// </summary>
        public ICommand DeleteSelectedUser => new DelegateCommand(() =>
        {
            if (_sessionUser.user.id != _userSelected.Id)
            {
                if (MessageBox.Show("Вы уверены, что хотите удалить пользователя?", "Удаление пользователя", MessageBoxButton.YesNo, MessageBoxImage.Warning) == MessageBoxResult.Yes)
                {
                    UserControl.DeleteCurrentUser(_userSelected.Id);
                    ListOfUsers = new List<User>();
                    ListOfUsers = UserControl.GetAllUsersList();
                    Users = new List<ItemUser>();
                    foreach (User user in ListOfUsers)
                    {
                        Users.Add(new ItemUser(user));
                    }
                }
            }
            else
            {
                MessageBox.Show("Вы не можете удалить пользователя текущей сессии!","Ошибка");
            }
        }, () => _userSelected != null);

        /// <summary>
        /// Команда для открытия окна создания нового отдела
        /// </summary>
        public ICommand OpenCreateNewDepartamentWindow => new DelegateCommand(() =>
        {
            DepartamentEditWindow departamentEditWindow = new DepartamentEditWindow();
            MessageService.Send(0);
            MessageService.Bus += Receive;

            if (departamentEditWindow.ShowDialog() == true)
            {
                if (_newDepartament.id == 0)
                {                    
                    DepartamentControl.InsertNewDepartament(_newDepartament);
                    ListDepartaments = new List<Departament>();
                    ListDepartaments = DepartamentControl.GetAllDepartamentsList();                    
                }
            }
            else
            {
                MessageService.Bus -= Receive;
            }
        });

        /// <summary>
        /// Командя для открытия окна редактирования отдела
        /// </summary>
        public ICommand OpenEditSelectedDepartamentWindow => new DelegateCommand(() =>
        {
            DepartamentEditWindow departamentEditWindow = new DepartamentEditWindow();
            MessageService.Send(_departamentSelected);
            MessageService.Bus += Receive;

            if (departamentEditWindow.ShowDialog() == true)
            {
                if (_newDepartament.id != 0)
                {                    
                    DepartamentControl.UpdateCurrentDepartament(_newDepartament);
                    ListDepartaments = new List<Departament>();
                    ListDepartaments = DepartamentControl.GetAllDepartamentsList();
                }
            }
            else
            {
                MessageService.Bus -= Receive;
            }
        }, () => _departamentSelected != null);

        /// <summary>
        /// Команда для удаления выбранного отдела
        /// </summary>
        public ICommand DeleteSelectedDepartament => new DelegateCommand(() =>
        {
                if (MessageBox.Show("Вы уверены, что хотите удалить отдел?", "Удаление отдела", MessageBoxButton.YesNo, MessageBoxImage.Warning) == MessageBoxResult.Yes)
                {
                    DepartamentControl.DeleteCurrentDepartament(_departamentSelected.id);
                    ListDepartaments = new List<Departament>();
                    ListDepartaments = DepartamentControl.GetAllDepartamentsList();
                 }
        }, () => _departamentSelected != null);
    }
}