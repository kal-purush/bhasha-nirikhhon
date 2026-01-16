using Sfs2X.Entities;

public class BoardInfo
{
    public bool isPlaying;
    public bool isLocked;
    public int sitCount;
    public bool isReqPlay;
    public int uCount;
    public int stake;
    public int minU;
    public bool ga;

    public void Update(Room r)
    {
        sitCount = r.GetVariable(GameConfig.VAR_SITCOUNT).GetIntValue();
        isPlaying = sitCount != -1;
        if (sitCount == -1)
        {
            sitCount = r.UserCount;
        }

        uCount = r.UserCount + r.SpectatorCount;
        stake = r.GetVariable(GameConfig.VAR_STAKE).GetIntValue();
        isLocked = r.IsPasswordProtected;

        var vMode = r.GetVariable(GameConfig.VAR_GAME_MODE);
        isReqPlay = vMode != null && vMode.GetIntValue() > 0;

        var vMin = r.GetVariable(GameConfig.VAR_MIN_U);
        minU = vMin?.GetIntValue() ?? 2;

        var vGa = r.GetVariable(GameConfig.VAR_IS_NUOI_GA);
        ga = vGa != null && vGa.GetBoolValue();
    }

    public void Reset(int zoneId)
    {
        isPlaying = isLocked = false;
        sitCount = uCount = 0;
        stake = GameConfig.ZoneCfg[zoneId].minStake;
        minU = 2;
        ga = true;
    }
}