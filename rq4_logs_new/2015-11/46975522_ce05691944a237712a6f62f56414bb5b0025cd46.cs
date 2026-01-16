using NotificationAgent.UI.Abstract;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;

namespace NotificationAgent
{
    public class NotificationsCenter<T> where T : GenericNotificationView, INotificationView, new()
    {
        #region Private constants

        private const string NOT_CONFIGURED_EXCEPTION_MESSAGE = "The behavior of the popups hasn't been configured!";

        #endregion

        #region Configuration fields

        private Queue<T> queuedPopupViews = new Queue<T>();
        private T[] activePopupViews = default(T[]);

        private bool isPositioningConfigured = false;
        private bool isDesignConfigured = false;

        #endregion

        #region Configuration properties

        private int _NotificationPositionX { get; set; }

        private Stream _NotificationSound { get; set; }

        private Color _NotificationColor { get; set; }

        private Color _TextColor { get; set; }

        private bool _IsConfigured
        {
            get
            {
                return isDesignConfigured && isPositioningConfigured;
            }
        }

        #endregion

        #region configuration methods

        public async Task SetupPopupsPositioning(Rectangle screenWorkingArea, Rectangle popUpArea)
        {
            await Task.Run(() =>
            {
                var rightMarginPointX = screenWorkingArea.X + screenWorkingArea.Width;
                var spacingFromRightMargin = popUpArea.Width / 4;
                _NotificationPositionX = rightMarginPointX - popUpArea.Width - spacingFromRightMargin;

                var maximumActivePopups = screenWorkingArea.Height / popUpArea.Height;
                activePopupViews = new T[maximumActivePopups];

                isPositioningConfigured = true;
            });
        }

        public async Task SetupPopupsDesign(Color notificationColor, Color textColor, Stream notificationSound)
        {
            await Task.Run(() =>
            {
                _NotificationColor = notificationColor;
                _TextColor = textColor;
                _NotificationSound = notificationSound;

                isDesignConfigured = true;
            });
        }

        #endregion
    }
}