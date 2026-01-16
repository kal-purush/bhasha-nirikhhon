using UnityEngine;
using UnityEditor;

public class MsgBase
{
    /// <summary>
    /// 错误码
    /// </summary>
    public int state;
    /// <summary>
    /// 消息编号
    /// </summary>
    public int code;
}
/// <summary>
/// 进入大厅
/// </summary>
public class Get_MainPage : MsgBase
{

}
/// <summary>
/// 商城
/// </summary>
public class Get_Shop : MsgBase
{

}
/// <summary>
/// 购买物品
/// </summary>
public class Post_PurchaseItem : MsgBase
{

}
/// <summary>
/// 俱乐部
/// </summary>
public class Get_Club : MsgBase
{

}
/// <summary>
/// 创建俱乐部
/// </summary>
public class Post_CreatClub : MsgBase
{

}
/// <summary>
/// 搜索俱乐部
/// </summary>
public class Get_SearchClub : MsgBase
{

}
/// <summary>
/// 申请俱乐部
/// </summary>
public class Post_ApplyClub : MsgBase
{

}