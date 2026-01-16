using KamranWali.CodeOptPro.Managers;
using KamranWali.CodeOptPro.ScriptableObjects.FixedVars;
using KamranWali.CodeOptPro.ScriptableObjects.RequestObjects;
using UnityEngine;

namespace KamranWali.CodeOptPro.Pools
{
    /// <summary>
    /// This class makes the children classes into pooling global system.
    /// </summary>
    /// <typeparam name="T">The object type to pool, of type <typeparamref name="T"/></typeparam>
    public abstract class BasePoolGlobal<T> : MonoAdvUpdateGlobal
    {
        [Header("BasePoolLocal Global Properties")]
        [SerializeField] protected BaseRequest<T> addRequest;
        [SerializeField, Tooltip("Size MUST be greater than 0.")] private FixedIntVar _sizeRequest;
        [SerializeField] private FixedBoolVar _enableAtStart;

        [Header("BasePoolLocal Local Properties")]
        [SerializeField] protected T[] poolObjects;

        protected int pointer_Request;
        private IRequestObject<T>[] _requests;
        private IRequestObject<T> _currentRequest;
        private int _pointer_Process;
        private int _pointer_PoolObject;
        protected int indexObj;
        private bool _isActive = false;

        public override void AwakeAdv()
        {
            SetRequestDelegate(); // Setting up the delegate
            _requests = new IRequestObject<T>[_sizeRequest.GetValue()];
            pointer_Request = 0;
            _pointer_Process = 0;
            _pointer_PoolObject = -1; // Making it -1 so that the first pool object is sent and NOT the second one
        }

        public override void StartAdv() => _isActive = _enableAtStart.GetValue();

        #region Editor Script
        public override void Init()
        {
            base.Init();
            poolObjects = new T[transform.childCount];
            SetupObjectPools(); // Setting up the object pools
        }
        #endregion

        public override bool IsActive() => _isActive;
        public override void SetActive(bool isActive) => _isActive = isActive;

        public override void UpdateObject()
        {
            if (_requests[_pointer_Process] != null) // Checking if any request is available
            {
                _currentRequest = _requests[_pointer_Process];
                _requests[_pointer_Process] = null; // Removing the request
                ProcessRequest();
                _pointer_Process = _pointer_Process + 1 >= _sizeRequest.GetValue() ? 0 : _pointer_Process + 1;
            }
            else if (_pointer_Process != pointer_Request) _pointer_Process = pointer_Request; // Checking for any overlap
        }

        /// <summary>
        /// This method adds request to be processed.
        /// </summary>
        /// <param name="request">The request to add, of type <typeparamref name="T"/></param>
        public void AddRequest(IRequestObject<T> request)
        {
            _requests[pointer_Request] = request;
            pointer_Request = pointer_Request + 1 >= _sizeRequest.GetValue() ? 0 : pointer_Request + 1;
        }

        /// <summary>
        /// This method gets the current request.
        /// </summary>
        /// <returns>The current request of type, of type <typeparamref name="T"/></returns>
        protected IRequestObject<T> GetCurrentRequest() => _currentRequest;

        /// <summary>
        /// This method gets the available pool object.
        /// </summary>
        /// <returns>The available pool object, of type <typeparamref name="T"/></returns>
        protected T GetPoolObject() => poolObjects[_pointer_PoolObject = _pointer_PoolObject + 1 >= poolObjects.Length ? 0 : _pointer_PoolObject + 1];

        /// <summary>
        /// This method process the request.
        /// </summary>
        protected virtual void ProcessRequest() => _currentRequest.ReceivePoolObject(GetPoolObject());

        /// <summary>
        /// This method sets up the object pool at start up.
        /// </summary>
        protected virtual void SetupObjectPools()
        {
            for (indexObj = 0; indexObj < poolObjects.Length; indexObj++) // Loop for populating the pool array
                transform.GetChild(indexObj).TryGetComponent(out poolObjects[indexObj]);
        }

        /// <summary>
        /// This method sets the request delegate for the pool.
        /// </summary>
        protected virtual void SetRequestDelegate() => addRequest.SetAction(AddRequest);
    }
}