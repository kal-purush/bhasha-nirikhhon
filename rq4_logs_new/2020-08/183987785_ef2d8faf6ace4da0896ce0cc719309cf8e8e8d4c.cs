
public struct ObjectStateInitializeProperty
{
    public ActionMethod[] actionMethods;
    public ObjectStatusOperationMethod[] statusOperations;
    public TransitionDeciderProperty[] transitionProperties;
}

public struct TransitionDeciderProperty
{
    public JudgeConditionMethod[] conditionJudgeMethods;
    public int judgeType;
    public int transitionIndex;
}

/// <summary>
/// 状態を配列で所持する。
/// </summary>
public sealed class FieldObjectStateMachine
{
    private int currentStateIndex = 0;
    private FieldObjectState[] states = null;

    public void Init(ObjectStateInitializeProperty[] initializeProperties)
    {
        states = new FieldObjectState[initializeProperties.Length];
        int propertyCount = 0;
        foreach(var initializeProperty in initializeProperties)
        {
            var objectState = new FieldObjectState();
            objectState.Init(initializeProperty);
            states[propertyCount] = objectState;
            propertyCount++;
        }
    }

    /// <summary>
    ///  状態遷移
    /// </summary>
    /// <param name="actionStatus"></param>
    /// <param name="externalStatus"></param>
    /// <returns></returns>
    public bool TransitionUpdate(ActionInternalStatus actionStatus, ActionExternalStatus externalStatus)
    {
        int nextStateIndex = -1;
        if(states[currentStateIndex].CheckTransition(ref nextStateIndex, actionStatus, externalStatus))
        {
            states[nextStateIndex].BeginState();
            currentStateIndex = nextStateIndex;
            return true;
        }
        return false;
    }

    public FieldObjectState GetCurrentObjectState()
    {
        return states[currentStateIndex];
    }
}

/// <summary>
/// ステートマシンが持つ1つの状態
/// </summary>
public sealed class FieldObjectState
{
    private StateTransitionDecider[] transitionDeciders = null;
    public ActionMethod[] actionMethods = null;
    public ObjectStatusOperationMethod[] objectStatusOperations = null;

    /// <summary>
    /// オブジェクト生成時に呼ぶ
    /// </summary>
    /// <param name="initializeProperty"></param>
    public void Init(in ObjectStateInitializeProperty initializeProperty)
    {
        // 行動関係の情報セット
        this.actionMethods = initializeProperty.actionMethods;
        this.objectStatusOperations = initializeProperty.statusOperations;

        // 状態遷移用情報のセット
        var transitionProperties = initializeProperty.transitionProperties;
        transitionDeciders = new StateTransitionDecider[transitionProperties.Length];        
        int transitionDeciderIndex = 0;
        foreach(var transitionProperty in transitionProperties)
        {
            var decider = new StateTransitionDecider();
            decider.Init(in transitionProperty);
            transitionDeciders[transitionDeciderIndex] = decider;
            transitionDeciderIndex++;
        } 
    }

    public void BeginState()
    {
        foreach(var decider in transitionDeciders)
        {
            decider.BeginConditionMethods();
        }
    }

    public bool CheckTransition(ref int nextStateIndex, ActionInternalStatus actionStatus, ActionExternalStatus externalStatus)
    {
        foreach(var decider in transitionDeciders)
        {
            if(decider.CheckTransition(actionStatus, externalStatus))
            {
                nextStateIndex = decider.transitionIndex;
                return true; 
            }
        }
        return false;
    }
}

/// <summary>
/// 状態遷移を行う
/// </summary>
public class StateTransitionDecider
{
    private int judgeType = 0; // sequenceなら全判定
    private JudgeConditionInterface[] judgeConditions = null;
    public int transitionIndex = 0;

    public void Init(in TransitionDeciderProperty property)
    {
        this.judgeType = property.judgeType;
        this.transitionIndex = property.transitionIndex;

        var conditionJudgeMethods = property.conditionJudgeMethods;
        this.judgeConditions = new JudgeConditionInterface[conditionJudgeMethods.Length];
        int conditionCount = 0;
        foreach(var conditionMethod in conditionJudgeMethods)
        {
            this.judgeConditions[conditionCount] = ConditionFactory.CreateCondition(conditionMethod);
            conditionCount++;
        }
    }

    public void BeginConditionMethods()
    {
        foreach(var judge in judgeConditions)
        {
            judge.Begin();
        }
    }

    public bool CheckTransition(ActionInternalStatus actionStatus, ActionExternalStatus externalStatus)
    {
        foreach(var judge in judgeConditions)
        {
            if(! judge.CheckTransition(actionStatus, externalStatus))
            {
                return false;
            }
        }
        return true;
    }
}