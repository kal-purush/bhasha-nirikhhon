using Android.App;
using Android.Widget;
using Android.OS;

namespace AndroidTest.Net45
{
    /// <summary>
    /// メインアクティビティを定義します。
    /// </summary>
    [Activity(Label = "AndroidTest.Net45", MainLauncher = true)]
    public class MainActivity : Activity
    {
        // 非公開フィールド

        // コンストラクタ

        /// <summary>
        /// 
        /// </summary>
        public MainActivity()
        {

        }

        
        // 非公開メソッド

        // 限定公開メソッド

        /// <summary>
        /// アクティビティが初期化された直後に実行されます。
        /// </summary>
        /// <param name="savedInstanceState"></param>
        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);

            // Set our view from the "main" layout resource
            SetContentView(Resource.Layout.Main);
        }
    }
}
