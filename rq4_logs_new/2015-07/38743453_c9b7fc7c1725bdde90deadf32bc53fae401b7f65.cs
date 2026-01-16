using System;

using Xamarin.Forms;

using Google.Maps;

// CustomMapをCustomMapRendererで扱う宣言をする
[assembly:ExportRenderer(typeof(GoogleMapSample.CustomMap), typeof(GoogleMapSample.CustomMapRenderer))]
namespace GoogleMapSample
{
	public class CustomMapRenderer : Xamarin.Forms.Maps.iOS.MapRenderer
	{
		/// <summary>
		/// Mapの生成時に呼び出される
		/// </summary>
		/// <param name="e">E.</param>
		protected override void OnElementChanged(Xamarin.Forms.Platform.iOS.ElementChangedEventArgs<View> e)
		{
			base.OnElementChanged(e);

			if (Element == null)
				return;

			// Elementをキャストして、共通クラスで定義したMapを取得
			var formsMap = (CustomMap)Element;

			// ↑で取得したマップから、初期の中心座標を取得
			var position = formsMap.firstPosition;

			// カメラ位置(中心座標と表示領域)を設定
			var camera = CameraPosition.FromCamera(position.Latitude, position.Longitude, 10);

			// マップを作成
			var mapView = MapView.FromCamera(CoreGraphics.CGRect.Empty, camera);

			// 現在地を表示
			mapView.MyLocationEnabled = true;

			// 現在位置へ移動ボタンを表示
			mapView.Settings.MyLocationButton = true;

			// コントロールに作成したMapViewを設定
			// これにより、ここで生成したMapViewがFormsのマップの代わりに扱われるようになる
			SetNativeControl(mapView);
		}
	}
}

