using UniRx;
using UnityEngine;

public class ViveInput : MonoBehaviour {

    private SteamVR_TrackedObject trackedObj;
    private SteamVR_Controller.Device Controller {
        get { return SteamVR_Controller.Input((int)trackedObj.index); }
    }

    public IObservable<Unit> ExplodeSaber;

    void Awake() {
        trackedObj = GetComponent<SteamVR_TrackedObject>();
        ExplodeSaber = Observable
            .EveryUpdate()
            .Select(_ => Controller.GetPressDown(SteamVR_Controller.ButtonMask.Touchpad))
            .DistinctUntilChanged()
            .Where(touchDown => touchDown)
            .Select(_ => Unit.Default)
            .AsObservable();
    }
}