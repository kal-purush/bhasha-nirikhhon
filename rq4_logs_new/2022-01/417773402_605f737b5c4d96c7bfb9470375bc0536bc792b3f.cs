using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Game.Characters.Interfaces
{
    public interface IAction<T>
    {
        bool isActive { get; }
        T target { get; }

        void ForceStart();
        void End();
    }

    public interface IOnAssignEvent
    {
        void OnAssign(CharacterBase character);
    }

    public interface ICloneable
    {
        int instanceId { get; }
        bool isCopied { get; }
        T CreateClone<T>() where T : Object, ICloneable;
        void InitializeClone(int instanceid);
    }

    public interface ICoolDown
    {
        float coolDownTime { get; }
        float currentCoolDownTime { get; }
        bool isCoolingDown { get; }
    }
    
    public interface IUltimate
    {
        Ultimate ultimate { get; }
        bool UseUltimate();
    }

    public interface ISkill
    {
        Skill skill { get; }
        bool UseSkill();
    }

    public interface IAttack
    {
        Attack attack { get; }
        bool UseAttack();
    }

    public interface IIcon
    {
        Sprite icon { get; }
    }
}