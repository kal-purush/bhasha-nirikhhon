using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.Events;
using UnityEngine.UI;
#if UNITY_EDITOR
using UnityEditor;
using UnityEditor.UI;
#endif

namespace Flappy.UI
{
	/// <summary>
	/// Button の拡張クラス
	/// </summary>
	/// <remarks>
	/// 長押しの対応や押下時の効果音を設定できる
	/// </remarks>
	public class ButtonEx : Button
	{
		/// <summary>
		/// 長押し反応時間
		/// </summary>
		public float LongPressTime { get; set; } = 0.4f;
		/// <summary>
		/// 長押しを続けた場合にイベントを繰り返すか
		/// </summary>
		public bool IsRepeatLongPress { get; set; } = true;
		/// <summary>
		/// 長押し繰り返し時間
		/// </summary>
		public float LongPressInterval { get; set; } = 0.5f;
		/// <summary>
		/// 長押しの繰り返しを徐々に早くするか
		/// </summary>
		public bool IncreaseRepeatRateOnLongPress { get; set; } = true;
		/// <summary>
		/// 長押し繰り返し間隔の最小時間
		/// </summary>
		public float RepeatRateMin
		{
			get
			{
				return this.repeatRateMin;
			}
			set
			{
				if (this.LongPressInterval < value)
				{
					throw new UnityException("invalid value is being set.");
				}
				this.repeatRateMin = value;
			}
		}
		private float repeatRateMin { get; set; } = 0.08f;
		/// <summary>
		/// 長押し繰り返し間隔が最小になるまでの時間
		/// </summary>
		public float IntervalLerpTime { get; set; } = 1f;
		/// <summary>
		/// ボタン押下時イベント
		/// </summary>
		public UnityEvent onButtonDown;
		/// <summary>
		/// ボタン離した時のイベント
		/// </summary>
		public UnityEvent onButtonUp;
		/// <summary>
		/// ボタン長押し時イベント
		/// </summary>
		public UnityEvent onButtonLongPressed;

		// 押下状態などの管理用
		bool isPressed = false;
		float pressTime = 0f;
		float intervalTime = 0f;
		float intervalLeapMag = 0f;
		bool isCalledLongPressEvent = false;

		void Start()
		{
			this.intervalLeapMag = 1.0f / this.IntervalLerpTime;
		}

		void Update()
		{
			if (this.isPressed)
			{
				this.UpdateLongPress();
			}
		}

		public override void OnPointerDown(PointerEventData eventData)
		{
			base.OnPointerDown(eventData);
			this.isPressed = true;
			this.pressTime = 0f;
			this.intervalTime = -this.LongPressInterval;
			this.isCalledLongPressEvent = false;
			this.onButtonDown?.Invoke();
		}

		public override void OnPointerExit(PointerEventData eventData)
		{
			base.OnPointerExit(eventData);
			this.isPressed = false;
			this.pressTime = 0f;
			this.intervalTime = -this.LongPressInterval;
		}

		public override void OnPointerUp(PointerEventData eventData)
		{
			base.OnPointerUp(eventData);
			this.onButtonUp?.Invoke();

			this.isPressed = false;
			this.pressTime = 0f;
			this.intervalTime = -this.LongPressInterval;
		}

		void UpdateLongPress()
		{
			if (this.IsRepeatLongPress == false && this.isCalledLongPressEvent == true)
			{
				return;
			}

			if (this.pressTime > this.LongPressTime)
			{
				this.isCalledLongPressEvent = true;
				this.onButtonLongPressed?.Invoke();
				if (this.IncreaseRepeatRateOnLongPress)
				{
					// 長押し時に繰り返し間隔を徐々に早くする
					this.pressTime -= Mathf.Lerp(this.LongPressInterval, this.repeatRateMin, Mathf.Min(1f, this.intervalTime * this.intervalLeapMag));
				}
				else
				{
					this.pressTime -= this.LongPressInterval;
				}
			}
			this.pressTime += Time.deltaTime;
			this.intervalTime += Time.deltaTime;
		}
	}

#if UNITY_EDITOR
	// Inspector からイベントを登録できるようにする
	[CanEditMultipleObjects, CustomEditor(typeof(ButtonEx), true)]
	public class ButtonExEditor : ButtonEditor
	{
		public override void OnInspectorGUI()
		{
			base.OnInspectorGUI();
			this.serializedObject.Update();
			EditorGUILayout.PropertyField(this.serializedObject.FindProperty("onButtonDown"), true);
			EditorGUILayout.PropertyField(this.serializedObject.FindProperty("onButtonUp"), true);
			EditorGUILayout.PropertyField(this.serializedObject.FindProperty("onButtonLongPressed"), true);
			this.serializedObject.ApplyModifiedProperties();
		}
	}
#endif

}