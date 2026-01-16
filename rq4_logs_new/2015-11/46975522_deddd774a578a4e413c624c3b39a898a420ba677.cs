using System.Drawing;
using System.Threading.Tasks;

namespace NotificationAgent.UI.Abstract
{
    public interface IDisplayConfigurator<TNotificationView> where TNotificationView : GenericNotificationView, INotificationView
    {
        #region Configuration

        bool IsConfigured { get; set; }

        Task SetupNotificationViewPositioning(Rectangle notificationViewArea);

        #endregion

        #region View positioning

        Task DisplayView(TNotificationView view);

        #endregion
    }
}