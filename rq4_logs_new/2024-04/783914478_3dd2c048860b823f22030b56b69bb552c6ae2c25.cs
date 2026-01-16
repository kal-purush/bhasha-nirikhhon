using System.Net.Mail;
using System.Text.RegularExpressions;
using LogicLayer.Cryptography;
using LogicLayer.Interfaces;
using LogicLayer.Models;

namespace LogicLayer.Core;

public static partial class Core
{

    

    
    // ReSharper disable once NullableWarningSuppressionIsUsed
    private static IDatabaseEntityService<Account> _accountService = null!;
    private static bool _initialized;
    
    public static void Init(IDatabaseEntityService<Account> accountService)
    {
        _accountService = accountService;
        _initialized = true;
    }
    
    
    
    //Private methods
    private static void CheckInit()
    {
        if (!_initialized) throw new Exception("Core not initialized");
    }
    
}