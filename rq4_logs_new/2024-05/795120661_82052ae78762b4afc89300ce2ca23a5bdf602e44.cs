#nullable enable
namespace Daipan.Viewer.Scripts
{
    public class DaipanExecutor
    {
        readonly DaipanParameter _daipanParameter;
        readonly ViewerNumber _viewerNumber;
        
        public DaipanExecutor(DaipanParameter daipanParameter, ViewerNumber viewerNumber)
        {
            _daipanParameter = daipanParameter;
            _viewerNumber = viewerNumber;
        }
        
        public void DaiPan()
        {
            _viewerNumber.IncreaseViewer(_daipanParameter.increaseNumberByDaipan);
        }
    }
}