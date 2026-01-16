using System;
using System.Collections;
using Object = UnityEngine.Object;


namespace JevLogin
{
    public sealed class ListExecuteObject : IEnumerator, IEnumerable
    {
        #region Fields
        
        private InteractiveObject _current;
        private IExecute[] _interactiveObjects;

        private int _index = -1;

        #endregion


        #region Property

        public ListExecuteObject()
        {
            var interactiveObjects = Object.FindObjectsOfType<InteractiveObject>();
            for (var i = 0; i < interactiveObjects.Length; i++)
            {
                if (interactiveObjects[i] is IExecute interactiveObject)
                {
                    AddExecuteObject(interactiveObject);
                }
            }
        }

        public int Length => _interactiveObjects.Length;

        public object Current => _interactiveObjects[_index];

        #endregion


        #region Methods

        private void AddExecuteObject(IExecute execute)
        {
            if (_interactiveObjects == null)
            {
                _interactiveObjects = new[] { execute };
                return;
            }
            Array.Resize(ref _interactiveObjects, Length + 1);
            _interactiveObjects[Length - 1] = execute;
        }

        public IEnumerator GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool MoveNext()
        {
            if (_index == _interactiveObjects.Length - 1)
            {
                Reset();
                return false;
            }

            _index++;
            return true;
        }

        public void Reset() => _index = -1; 

        #endregion
    }
}