using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace SnowFrameWork{
	public abstract class BaseUI : MonoBehaviour {

		#region Cache gameObject & transform

		private GameObject _cacheGameObject;
		/// <summary>
		/// Gets the cache game object.
		/// </summary>
		/// <value>The cache game object.</value>
		public GameObject CacheGameObject
		{
			get{ 
				if(null == _cacheGameObject)
					_cacheGameObject = this.gameObject;
				return _cacheGameObject;
			}
		}

		private Transform _cacheTransform;
		/// <summary>
		/// Gets the cache transform.
		/// </summary>
		/// <value>The cache transform.</value>
		public Transform CacheTransform
		{
			get{ 
				if (null == _cacheTransform)
					_cacheTransform = this.transform;
				return _cacheTransform;
			}
		}
		#endregion

		#region EnumObjectState & UIType

		protected EnumObjectState _state = EnumObjectState.None;

		public event StateChangeEvent StateChanged;

		/// <summary>
		/// Gets or sets the state.
		/// </summary>
		/// <value>The state.</value>
		public EnumObjectState State
		{
			protected get{ 
				return this._state; 
			}
			set{
				EnumObjectState oldState = this._state;
				this._state = value; 

				if (null != StateChanged) {
					StateChanged (this, this._state, oldState);
				}
			}

		}
		/// <summary>
		/// Gets the type of the user interface.
		/// </summary>
		/// <returns>The user interface type.</returns>
		public abstract EnumUIType GetUIType ();

		#endregion

		// Use this for initialization
		void Start () {
			OnStart ();	
		}

		void Awake(){
			this.State = EnumObjectState.Initial;
			OnAwake ();
		}
		// Update is called once per frame
		void Update () {
			if (this._state == EnumObjectState.Ready) {
				OnUpdate (Time.deltaTime);
			}
		}

		void Release(){
			this.State = EnumObjectState.Closing;
			GameObject.Destroy (this.CacheGameObject);
			OnRelease ();
		}

		void Destroy()
		{
			this.State = EnumObjectState.None;
		}

		protected virtual void OnStart () {}
		protected virtual void OnAwake () {
			this.State = EnumObjectState.Loading;
			//play music
			this.OnPlayOpenUIAudio();
		}
		protected virtual void OnUpdate ( float deltaTime ) {}
		protected virtual void OnRelease () {
			this.State = EnumObjectState.None;
			//close music
			this.OnPlayCloseUIAudio();
		}
		protected virtual void OnLoadData() {}

		protected virtual void OnPlayOpenUIAudio () {}
		protected virtual void OnPlayCloseUIAudio () {}

		/// <summary>
		/// Sets the U.
		/// </summary>
		/// <param name="uiParams">User interface parameters.</param>
		protected virtual void SetUI(params object[] uiParams)
		{
			this.State = EnumObjectState.Loading;
		}

		public void SetUIWhenOpening( params object[] uiParams)
		{
			SetUI (uiParams);
			StartCoroutine (LoadDataAsyn ());
		}

		private IEnumerator LoadDataAsyn()
		{
			yield return new WaitForSeconds (0);
			if (this.State == EnumObjectState.Loading) {
				this.OnLoadData ();
				this.State = EnumObjectState.Ready;
			}
		}
	}
}