using Standard.Tool.Platform.Auth.PermissionFeature;
using Standard.Tool.Platform.Data.Entities;
using System;
using System.Collections.Generic;

namespace Standard.Tool.Platform.Auth.AccountFeature;

public class Account
{
    private int _no;
    /// <summary>
    /// 序号
    /// </summary>
    public int No
    {
        get { return _no; }
        set
        {
            _no = ++value;
        }
    }

    /// <summary>
    /// 是否选中
    /// </summary>
    public int IsSelected { get; set; }

    /// <summary>
    /// 
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// 用户名
    /// </summary>
    public string UserName { get; set; }

    /// <summary>
    /// 登录名
    /// </summary>
    public string LoginName { get; set; }

    /// <summary>
    /// 权限
    /// </summary>
    public List<Permission> Permissions { get; set; }

    /// <summary>
    /// 最后登录时间
    /// </summary>
    public DateTime? LastLoginTimeUtc { get; set; }

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreateTimeUtc { get; set; }

    public string Status { get; set; }

    public Account()
    {

    }

    public Account(AccountEntity entity)
    {
        if (null == entity) return;
        Id = entity.Id;
        CreateTimeUtc = entity.CreateTimeUtc;
        LastLoginTimeUtc = entity.LastLoginTimeUtc.GetValueOrDefault();
        UserName = entity.UserName.Trim();
        Status = entity.Status.Trim();
        //Permissions = entity.Permissions.Select(sm => new Permission
        //{
        //    Id = sm.Id,
        //    Name = sm.Name,
        //    Code = sm.Code,
        //    Status=sm.Status,
        //    LastModifiedTimeUtc=sm.LastModifiedTimeUtc,
        //}).ToList();
    }
}

public class StatusValue
{
    public string Name { get; set; }

    public string Value { get; set; }
}