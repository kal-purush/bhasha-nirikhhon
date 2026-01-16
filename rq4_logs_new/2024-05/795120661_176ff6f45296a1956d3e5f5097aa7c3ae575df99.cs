using Stream.Viewer.Scripts;
using UnityEngine;
using VContainer;

namespace Daipan.Stream.MonoScripts
{
    public class StreamMono : MonoBehaviour
    {
        StreamStatus _streamStatus;
        ViewerNumber _viewerNumber;
        ViewerParameter _viewerParameter;

        float Timer { get; set; }

        void Update()
        {
            Timer += Time.deltaTime;
            if (Timer > 1)
            {
                if (_streamStatus.ExistIrrationalFactors)
                    _viewerNumber.DecreaseViewer(_viewerParameter.decreaseNumberWhenIrradiated);
                else
                    _viewerNumber.IncreaseViewer(_viewerParameter.increaseNumberPerSecond);
                Timer = 0;
            }
        }

        [Inject]
        public void Initialize(
            ViewerParameter viewerParameter,
            ViewerNumber viewerNumber,
            StreamStatus streamStatus)
        {
            _viewerParameter = viewerParameter;
            _viewerNumber = viewerNumber;
            _streamStatus = streamStatus;
        }
    }
}