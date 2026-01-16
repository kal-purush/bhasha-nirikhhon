using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class EffectorManager {

    //variable
    List<Effector> effectList;

    public EffectorManager(int effectInitNo = 5) {
        effectList = new List<Effector>(effectInitNo);
    }

    public void RunAllEffect() {
        for (int i = 0; i < effectList.Count; ++i) {
            effectList[i].OperateEffect();
        }
    }

    public void AddEffector(Effector effector, Object parent) {
        effector.Parent = parent;
        effectList.Add(effector);
    }

    public void RemoveEffector(System.Type type, int value) {
        for (int i = 0; i < effectList.Count; ++i) {
            if (effectList[i].IsSameEffectorWithTypeAndValue(type, value)) {
                effectList.RemoveAt(i);
                break;
            }
        }
    }
}

public abstract class Effector {

    //variable
    protected int value;
    protected Object parent;
    public Object Parent {
        set {
            parent = value;
        }
    }

    public Effector(int value) {
        this.value = value;
    }
    public abstract void OperateEffect();

    public bool IsSameEffectorWithTypeAndValue(System.Type type, int value) {
        return (this.GetType() == type) && (this.value == value);
    }
}

public class DestroyItemEffector : Effector {

    //variable
    int effectNo = 0;

    public DestroyItemEffector(int value)
        : base(value) {
        
    }

    public override void OperateEffect() {
        if (value <= ++effectNo) {
            DestroyItem();
        }
    }

    void DestroyItem() {
        effectNo = 0;
        (parent as Item).DestroyItem();
    }
}

public class DamageEffector : Effector {

    public DamageEffector(int value)
        : base(value) {
        
    }

    public override void OperateEffect() {

    }
}

public class ScoreEffector : Effector {

    public ScoreEffector(int value)
        : base(value) {
        
    }

    public override void OperateEffect() {

    }
}

public class GainEffector : Effector {

    public GainEffector(int value)
        : base(value) {
        
    }

    public override void OperateEffect() {

    }
}