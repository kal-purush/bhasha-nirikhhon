using System.Collections.Generic;
using UnityEngine;

namespace TAMKShooter.Systems
{
	public abstract class GenericPool<T> : MonoBehaviour
		where T : Component
	{
		[SerializeField] private int _objectAmount;
		[SerializeField] private T _objectPrefab;
		[SerializeField] private bool _shouldGrow;

		private List<T> _pool;
		private List<bool> _isActive;

		protected void Awake()
		{
			_pool = new List<T> ( _objectAmount );
			_isActive = new List<bool> ( _objectAmount );

			for(int i = 0; i < _objectAmount; ++i )
			{
				AddItemToPool ();
			}
		}

		/// <summary>
		/// Creates an item and adds it to pool.
		/// </summary>
		/// <param name="activate">Should object be active or not.</param>
		/// <returns>The added object.</returns>
		private T AddItemToPool ( bool activate = false )
		{
			T obj = Instantiate ( _objectPrefab );
			if(!activate)
			{
				Deactivate ( obj );
			}

			_pool.Add ( obj );
			_isActive.Add ( activate );

			var index = _pool.IndexOf ( obj );

			Debug.Log ( string.Format ( "{0} added to pool. Is active: {1}",
				obj, _isActive[index] ) );

			return obj;
		}

		/// <summary>
		/// Returns an inactive object from pool.
		/// </summary>
		/// <returns>If pool has objects or it can grow, returns an object. Otherwise returns null.</returns>
		public T GetPooledObject()
		{
			Debug.Log ( "Get pooled object" );

			T result = null;
			for(int i = 0; i < _isActive.Count; ++i )
			{
				if(!_isActive[i])
				{
					result = _pool[i];
					_isActive[i] = true;
					break;
				}
			}

			if(result == null && _shouldGrow)
			{
				result = AddItemToPool ( true );
			}

			return result;
		}

		/// <summary>
		/// Returns object back to pool, ie. sets its state to inactive.
		/// </summary>
		/// <param name="obj">The object which is returned to pool.</param>
		public void ReturnObjectToPool(T obj)
		{
			for(int i = 0; i < _pool.Count; ++i )
			{
				if(_pool[i] == obj)
				{
					_isActive[i] = false;
					Deactivate ( obj );
					break;
				}
			}
		}

		protected virtual void Deactivate ( T item )
		{
			item.gameObject.SetActive ( false );
		}
	}
}