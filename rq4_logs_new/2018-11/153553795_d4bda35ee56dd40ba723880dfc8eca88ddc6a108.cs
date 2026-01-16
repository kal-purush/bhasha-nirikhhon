using GameFramework;
using GameFramework.Fsm;
using GameFramework.Procedure;

public class PlayerStateController : IPlayerStateController
{
    private IFsmManager m_FsmManager;
    private IFsm<IPlayerStateController> m_ProcedureFsm;

    public void Init(IFsmManager FSM,params PlayerStateBase[] states)
    {
        if (FSM == null)

        {

            throw new GameFrameworkException("FSM manager is invalid.");

        }



        m_FsmManager = FSM;

        m_ProcedureFsm = m_FsmManager.CreateFsm(this, states);
        
    }
    public void FireEvent()
    {
        
    }
}