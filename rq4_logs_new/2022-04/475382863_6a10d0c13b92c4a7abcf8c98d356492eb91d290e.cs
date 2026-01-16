using UniRx;
using UnityEngine;

namespace MVVMFramework.Bindings
{
    public abstract class BindingBase : MonoBehaviour
    {
        public ViewModel ViewModel;
        public string ViewModelProperty;
        public GameObject Target;
        
        protected CompositeDisposable disposables = new CompositeDisposable();
        
        void OnDestroy() => disposables.Clear();

        public void OnValidate() => Initialize();

        protected virtual void Initialize()
        {
            disposables.Clear();
            if (!IsValid()) return;

            UpdateComponentOnChange(new PropertyChanged(ViewModelProperty, ViewModel.GetValueOf(ViewModelProperty)));
            BindToViewModel();
        }
        
        protected void BindToViewModel() =>
            ViewModel.onPropertyChanged
                .Where(propertyChanged => propertyChanged.Property.Equals(ViewModelProperty))
                .Subscribe(UpdateComponentOnChange)
                .AddTo(disposables);

        protected abstract void UpdateComponentOnChange(PropertyChanged changed);

        protected virtual bool IsValid() => 
            Target != null && 
            ViewModel != null && 
            !string.IsNullOrEmpty(ViewModelProperty);
    }
}