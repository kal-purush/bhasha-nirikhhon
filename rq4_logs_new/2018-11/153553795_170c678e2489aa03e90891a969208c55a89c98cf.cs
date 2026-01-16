using System.Collections;
using System.Collections.Generic;

public class GameManager : MonoSingleton<GameManager>
{
    RoleData roleData;
    Send_MsgBase sendBaseData;

    public void InitRoleData(string id, string token)
    {
        GetRoleData().InitData(id, token);

        if (sendBaseData == null)
        {
            sendBaseData = new Send_MsgBase();
        }
    }

    public RoleData GetRoleData()
    {
        if (roleData == null)
        {
            roleData = new RoleData();
        }

        return roleData;
    }

    public List<string> GetSendInfoStringList<T>(params object[] args) where T : Send_MsgBase, new()
    {
        var temp = new T();

        return temp.CreateSendInfo(args);
    }
}