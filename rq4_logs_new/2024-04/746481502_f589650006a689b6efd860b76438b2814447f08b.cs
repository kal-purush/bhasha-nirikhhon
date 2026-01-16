using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Threading.Tasks;
using Legion.Models;
using Microsoft.EntityFrameworkCore;
using ReactiveUI;
using ReactiveUI.Validation.Extensions;
using Serilog;
using Splat;

namespace Legion.ViewModels
{
    public class AddRoleViewModel : ViewModelBase
    {
        private readonly ApplicationDbContext _context;
        private UserRole _userRole = null!;

        public AddRoleViewModel(UserRole userRole, ApplicationDbContext context, IScreen? hostScreen = null) : this(
            context, hostScreen)
        {
            UserRole = userRole;
            SubmitText = "Редактировать роль";
            _context.UserRoles.Load();

            SaveCommand = ReactiveCommand.Create(() =>
            {
                _context.UserRoles.Update(UserRole);

                try
                {
                    _context.SaveChanges();
                }
                catch (Exception ex)
                {
                    Log.Error(ex.Message);
                }
                finally
                {
                    BackCommand.Execute();
                }
            });


        }

        public AddRoleViewModel(ApplicationDbContext context, IScreen? hostScreen = null)
        {
            HostScreen = hostScreen ?? Locator.Current.GetService<IScreen>()!;
            _context = context;
            UserRole = new UserRole();
            SubmitText = "Добавить роль";

            BackCommand = ReactiveCommand.Create(() =>
            {
                HostScreen.Router.NavigateBack.Execute();
            });

            SaveCommand = ReactiveCommand.Create(() =>
            {
                _context.UserRoles.Add(UserRole);

                try
                {
                    _context.SaveChanges();
                    HostScreen.Router.NavigateBack.Execute();
                }
                catch (Exception ex)
                {
                    Log.Error(ex.Message);
                }
            });
        }

        public ReactiveCommand<Unit, Unit> BackCommand { get; } = null!;
        public ReactiveCommand<Unit, Unit> SaveCommand { get; }
        public string SubmitText { get; protected set; }
        public UserRole UserRole { get => _userRole; set => this.RaiseAndSetIfChanged(ref _userRole, value); }
        public sealed override IScreen HostScreen { get; set; }
    }
}