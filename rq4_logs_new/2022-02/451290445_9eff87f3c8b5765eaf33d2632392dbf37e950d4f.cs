using Sfs2X.Entities.Data;
using UnityEngine;

public class InitGameInExt : MonoBehaviour
{
    public long coin;
    public int exp;
    public int giftCount;
    public int vipScore;
    public int debt;
    public int roundPlay;

    public int[][] arrStake;
    public ZoneInfo[] arrZone;

    public InitGameInExt(ISFSObject obj)
    {
        coin = obj.GetLong("c");
        exp = obj.GetInt("e");
        giftCount = obj.GetInt("g");
        vipScore = obj.GetInt("v");
        debt = obj.GetInt("d");

        roundPlay = obj.GetInt("rp");

        arrStake = new int[6][];
        for(var i = 0; i < 6; i++)
        {
            arrStake[i] = obj.GetIntArray("s" + i);
        }

        var cfg = obj.GetSFSArray("cfg");
        if(cfg != null && cfg.Size() > 0)
        {
            arrZone = new ZoneInfo[cfg.Size() + 1];
            for(var i = 0; i < cfg.Size(); i++)
            {
                arrZone[i] = GetZoneInfo(cfg.GetSFSObject(i));
            }
        }

        if(obj.ContainsKey("a"))
        {
            arrZone[arrZone.Length - 1] = GetZoneInfo(obj.GetSFSObject("a"));
        }
    }

    public static ZoneInfo GetZoneInfo(ISFSObject o)
    {
        return new ZoneInfo
        {
            name = o.GetUtfString("n"),
            minExp = o.GetInt("me"),
            level = o.GetInt("lv"),
            minStake = o.GetInt("s"),
            maxStake = o.GetInt("ms"),
            stakePerLevel = o.GetInt("spl"),
            coinToJoin = o.GetInt("cj"),
            vipToJoin = o.GetInt("vtj"),
            rooms = o.GetUtfStringArray("r"),
            desc = o.GetUtfString("d"),
            isAutoXuong = o.GetBool("ax"),
            canLockBoard = o.GetBool("lb"),
            nuoiGa = o.GetBool("ng"),
            canManuallySortCards = o.GetBool("msc"),
            showCardName = o.GetBool("scn"),
            canXinChoiTinhDiem = o.GetBool("xctd"),
            canULao = o.GetBool("ul"),
            canSetMinScoreU = o.GetBool("cmu")
        };
    }
}