using MediatR;
using Standard.Tool.Platform.Auth;
using Standard.Tool.Platform.MVVM;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows.Input;
using AuthCore = Standard.Tool.Platform.Auth;

namespace Standard.Tool.Platform.Pages.Account;

public class AccountPageViewModel : ObservableObject
{
    private readonly IMediator _mediator;
    public AccountPageViewModel(IMediator mediator)
    {
        _mediator = mediator;
        SearchExecute();
    }

    #region Property

    #region Status
    private StatusValue _status;
    public StatusValue Status
    {
        get { return _status; }
        set
        {
            _status = value;
            RaisePropertyChanged(nameof(Status));
        }
    }

    private ObservableCollection<StatusValue> _statusList;
    public ObservableCollection<StatusValue> StatusList
    {
        get { return _statusList; }
        set
        {
            _statusList = value;
            RaisePropertyChanged(nameof(StatusList));
        }
    }
    #endregion

    #region UserList
    private ObservableCollection<AuthCore.Account> _userList;

    public ObservableCollection<AuthCore.Account> UserList
    {
        get { return _userList; }
        set
        {
            _userList = value;
            RaisePropertyChanged(nameof(UserList));
        }
    }
    #endregion

    #endregion


    #region Method

    #region 01，Refresh
    void RefreshExecute()
    {

    }

    bool CanRefreshExecute()
    {
        return true;
    }

    public ICommand Refresh
    {
        get { return new RelayCommand(RefreshExecute, CanRefreshExecute); }
    }
    #endregion


    #region 02，Search
    void SearchExecute()
    {
        var statusData = new List<StatusValue>();
        statusData.Add(new StatusValue() { Name = "==请选择==", Value = "Enable" });
        statusData.Add(new StatusValue() { Name = "启用", Value = "Enable" });
        statusData.Add(new StatusValue() { Name = "禁用", Value = "Disable" });

        StatusList = new ObservableCollection<StatusValue>(statusData);
        IList<AuthCore.Account> dataList = null;
        Task.Run(async
            () =>
        {

            dataList = await _mediator.Send(new GetAccountsQuery());

            if (dataList != null)
                UserList = new ObservableCollection<AuthCore.Account>(dataList);
        });


    }

    bool CanSearchExecute()
    {
        return true;
    }

    public ICommand Search
    {
        get { return new RelayCommand(SearchExecute, CanSearchExecute); }
    }
    #endregion


    #endregion
}