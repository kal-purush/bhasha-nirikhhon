/// <summary>
/// ObjectActionInterface
/// </summary>
public struct ActionAccessProperty
{
    public int index;
    public ActionMethod method;
    public bool isActive;
    
    public void Init(int index, ActionMethod method)
    {
        this.index = index;
        this.method = method;
        this.isActive = false;
    }
}

/// <summary>
/// アクションを実行するクラス
/// ひとまず仮
/// オブジェクト生成後に使用するアクション情報を全部見て作られる
/// </summary>
public sealed class ActionExecuter
{
    private ActionAccessProperty[] accessProperties;
    private ObjectActionInterface[] actions;

    /// <summary>
    /// 受け取ったActionMethodから対応するActionを生成。アクセス用の構造体を初期化
    /// </summary>
    /// <param name="actionMethods"></param>
    public void Init(ActionMethod[] actionMethods)
    {
        int actionCount = actionMethods.Length;
        accessProperties = new ActionAccessProperty[actionCount];
        actions = new ObjectActionInterface[actionCount];
        for(int index = 0; index < actionCount; index++)
        {
            actions[index] = ActionFactory.CreateAction(actionMethods[index]);
            accessProperties[index].Init(index, actionMethods[index]);
        }
    }

    /// <summary>
    /// 実行する行動の切り替え
    /// </summary>
    /// <param name="enableMethods"> 起動するActionのリスト。 enumの番号順に揃っている事が前提。</param>
    public void SetExecuteActions(ActionMethod[] enableMethods)
    {
        int enableIndex = 0;
        int length = enableMethods.Length;
        int propertiesLength = accessProperties.Length;

        // 実行するアクションをenableMethodsを参照して起動。それ以外はOFF
        for(int index = 0; index < propertiesLength; index++)
        {
            if(enableIndex < length && accessProperties[index].method == enableMethods[enableIndex])
            {
                accessProperties[index].isActive = true;
                enableIndex++;
                continue;
            }
            accessProperties[index].isActive = false;
        }
    }

    /// <summary>
    /// 行動処理
    /// </summary>
    /// <param name="actionStatus"></param>
    /// <param name="externalStatus"></param>
    public void Action(ActionInternalStatus actionStatus, ActionExternalStatus externalStatus)
    {
        foreach(var property in accessProperties)
        {
            if(property.isActive)
            {
                actions[property.index].Action(actionStatus, externalStatus);
            }
        }
    }
}