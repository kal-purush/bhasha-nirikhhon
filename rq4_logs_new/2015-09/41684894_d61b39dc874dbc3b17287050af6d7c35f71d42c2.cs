using System.ComponentModel;
using System.Runtime.CompilerServices;
using DealCapture.Client.Annotations;

namespace DealCapture.Client.DealEnrichment
{
    public sealed class DealEnrichmentSection : INotifyPropertyChanged
    {


        #region INPC

        public event PropertyChangedEventHandler PropertyChanged;

        [NotifyPropertyChangedInvocator]
        private void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChangedEventHandler handler = PropertyChanged;
            if (handler != null) handler(this, new PropertyChangedEventArgs(propertyName));
        }

        #endregion

    }
}