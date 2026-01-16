namespace Defender.ViewModel
{
    public class ProgressItem : ViewModelBase
    {
        private int _prog;
        private int _total;
        private string _currentItem;

        public int Progress
        {
            get
            {
                return this._prog;
            }
            set
            {
                this._prog = value;
                this.RaisePropertyChanged(nameof(Progress));
            }
        }

        public string CurrentItem
        {
            get
            {
                return this._currentItem;
            }
            set
            {
                this._currentItem = value;
                this.RaisePropertyChanged(nameof(CurrentItem));
            }
        }

        public int TotalItems
        {
            get
            {
                return this._total;
            }
            set
            {
                this._total = value;
                this.RaisePropertyChanged(nameof(TotalItems));
            }
        }
    }
}