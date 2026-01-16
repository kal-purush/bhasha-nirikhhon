using System;
using GameFramework.Fsm;
using UniRx;
using UnityGameFramework.Runtime;
using PlayerOwner = GameFramework.Fsm.IFsm<Player>;

public class PlayerStateEnterRoom : PlayerStateBase
{
    protected override void OnInit(GameFramework.Fsm.IFsm<Player> fsm)
    {
        base.OnInit(fsm);
        Log.Debug("PlayerStateEnterRoom OnInit name={0}",fsm.Owner.name);
    }

    protected override void OnEnter(GameFramework.Fsm.IFsm<Player> fsm)
    {
        base.OnEnter(fsm);
        Log.Debug("PlayerStateEnterRoom Enter name={0}",fsm.Owner.name);
        fsm.Owner.OnEnterRoom();
    }

    protected override void OnUpdate(GameFramework.Fsm.IFsm<Player> fsm, float elapseSeconds, float realElapseSeconds)
    {
        base.OnUpdate(fsm, elapseSeconds, realElapseSeconds);
    }

    protected override void OnLeave(GameFramework.Fsm.IFsm<Player> fsm, bool isShutdown)
    {
        base.OnLeave(fsm, isShutdown);
    }

    protected override void OnDestroy(GameFramework.Fsm.IFsm<Player> fsm)
    {
        base.OnDestroy(fsm);
    }
}