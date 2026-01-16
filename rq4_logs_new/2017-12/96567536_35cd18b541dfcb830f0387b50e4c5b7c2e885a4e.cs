using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;

namespace AndroidTest.Net45
{
    /// <summary>
    /// 
    /// </summary>
    [Activity(Label = "Streams Test")]
    public class StreamsTestActivity : Activity
    {
        // 非公開フィールド


        // コンストラクタ

        /// <summary>
        /// 新しい StreamsTestActivity クラスのインスタンスを初期化します。
        /// </summary>
        public StreamsTestActivity()
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
            // アクティビティの共通初期処理
            base.OnCreate(savedInstanceState);
            this.SetContentView(Resource.Layout.StreamsTest);
        }
    }
}