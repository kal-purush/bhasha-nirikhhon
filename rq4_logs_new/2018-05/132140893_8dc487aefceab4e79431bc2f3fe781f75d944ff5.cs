using Android.App;
using Android.Content;
using Android.Media;
using Android.Views;
using Android.Widget;

namespace WorldAndroidRadio
{
    public class MediaPlayerManager
    {
        public static MediaPlayer player;
        public static ProgressDialog mDialog;
        public static async void Play(string Url, Context context)
        {
            if (player == null)
            {
                player = new MediaPlayer();
                player.SetAudioStreamType(Stream.Music);
            }
            else {
                player.Reset();
            }
            try
            {
                await player.SetDataSourceAsync(Url);
                player.PrepareAsync();
                mDialog = new ProgressDialog(context);
                mDialog.Window.SetType(WindowManagerTypes.SystemAlert);
                mDialog.SetMessage("Please wait...");
                mDialog.Show();
            }
            catch
            {
                Toast.MakeText(context, "Radio Station not Responding", ToastLength.Long).Show();
            }
            player.Prepared += Player_Prepared;
            player.Error += (sender, args) =>
            {
                Toast.MakeText(context, "Radio Station not Responding", ToastLength.Long).Show();
                mDialog.Dismiss();
                StopePlayer();
            };
        }

        private static void Player_Prepared(object sender, System.EventArgs e)
        {
            player.Start();
            mDialog.Dismiss();
        }

        public static bool CheckPlayer()
        {
            if (player != null)
            {
                if (player.IsPlaying)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        public static bool StopePlayer()
        {
            if (CheckPlayer())
            {
                player.Stop();
                player.Reset();
            }

            return true;
        }
    }
}