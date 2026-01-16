using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;

namespace SFLibs.Core.Adapters
{
	public class ViewModelsAdapter<TVM, TM>
	{
		private Dictionary<TM,TVM> dic = new Dictionary<TM, TVM>();

		private IList<TVM> viewmodels = null;
		private IList<TM> models = null;

		protected Func<TM,TVM> createViewModel;
		private Action<TVM> deleteVM;

		public ViewModelsAdapter( Func<TM, TVM> createVM, Action<TVM> deleteVM = null )
		{
			this.createViewModel = createVM;
			this.deleteVM = deleteVM;
		}

		public virtual void Adapt( IList<TVM> viewmodels, IList<TM> models )
		{
			this.viewmodels = viewmodels;
			this.models = models;

			foreach( var item in models.Select( ( v, i ) => new { v, i } ) )
			{
				var vm = CreateViewModel( item.i, item.v );
			}

			if( models is INotifyCollectionChanged )
			{
				( (INotifyCollectionChanged)models ).CollectionChanged += Models_CollectionChanged;
			}
		}

		protected virtual TVM CreateViewModel( int index, TM model )
		{
			var vm = createViewModel( model );
			this.viewmodels.Insert( index, vm );
			this.dic[model] = vm;

			return vm;
		}

		protected virtual void DeleteViewModel( TM model )
		{
			var vm = this.dic[model];
			this.dic.Remove( model );
			deleteVM?.Invoke( vm );
			this.viewmodels.Remove( vm );
		}

		protected virtual void Models_CollectionChanged( object sender, NotifyCollectionChangedEventArgs e )
		{
			if( e.OldItems != null )
			{
				foreach( TM item in e.OldItems )
				{
					DeleteViewModel( item );
				}
			}
			if( e.NewItems != null )
			{
				foreach( var item in e.NewItems.Cast<TM>().Select( ( v, i ) => new { v, i } ) )
				{
					CreateViewModel( e.NewStartingIndex + item.i, item.v );
				}
			}

			if( e.Action == NotifyCollectionChangedAction.Reset )
			{
				foreach( var model in this.dic.Keys.ToArray() )
				{
					DeleteViewModel( model );
				}
			}
		}
	}
}