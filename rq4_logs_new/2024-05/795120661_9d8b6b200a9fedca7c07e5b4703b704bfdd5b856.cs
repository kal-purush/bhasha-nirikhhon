using Stream.Viewer.Scripts;
using UnityEngine;
using VContainer;

namespace Daipan.Stream.MonoScripts
{
    public class ViewerMono : MonoBehaviour
    {
        DistributionStatus _distributionStatus;
        ViewerNumber _viewerNumber;
        ViewerNumberParameter _viewerNumberParameter;

        float Timer { get; set; }

        void Update()
        {
            Timer += Time.deltaTime;
            if (Timer > 1)
            {
                if (_distributionStatus.ExistIrrationalFactors)
                    _viewerNumber.DecreaseViewer(_viewerNumberParameter.decreaseNumberWhenIrradiated);
                else
                    _viewerNumber.IncreaseViewer(_viewerNumberParameter.increaseNumberPerSecond);
                Timer = 0;
            }
        }

        [Inject]
        public void Initialize(
            ViewerNumberParameter viewerNumberParameter,
            ViewerNumber viewerNumber,
            DistributionStatus distributionStatus)
        {
            _viewerNumberParameter = viewerNumberParameter;
            _viewerNumber = viewerNumber;
            _distributionStatus = distributionStatus;
        }
    }
}