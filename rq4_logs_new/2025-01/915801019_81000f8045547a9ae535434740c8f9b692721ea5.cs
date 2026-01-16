using FreeView.ViewModels;
using Sample.Scripts.Controllers;

namespace Sample.Scripts.ViewModels
{
    public class PlaygroundViewModel : BaseViewModel<PlaygroundNavigationArgs>
    {
        private DoorController _doorController;
        private bool _isDoorOpened;

        public bool IsDoorOpened
        {
            get => _isDoorOpened;
            set => SetProperty(ref _isDoorOpened, value);
        }
        
        public override void Prepare(PlaygroundNavigationArgs args)
        {
            base.Prepare(args);
            _doorController = args.DoorController;
            IsDoorOpened = _doorController.IsOpened;
        }
        
        public override void OnViewEnable()
        {
            base.OnViewEnable();
            _doorController.DoorStateChanged += OnDoorControllerStateChanged;
        }
        
        public override void OnViewDisable()
        {
            base.OnViewDisable();
            _doorController.DoorStateChanged -= OnDoorControllerStateChanged;
        }
        
        public void ToggleDoor()
        {
            _doorController.Toggle();
        }
        
        private void OnDoorControllerStateChanged(object sender, bool isOpened)
        {
            IsDoorOpened = isOpened;
        }
    }
}