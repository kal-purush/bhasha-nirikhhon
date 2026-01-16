using DevExpress.Blazor;
using Microsoft.AspNetCore.Components;

namespace DxBlazorComponentsDefaultSettings.Components {
    public class MyGrid : DxGrid {
        bool _initialParametersSet;
        protected override Task SetParametersAsyncCore(ParameterView parameters) {
            if (!_initialParametersSet) {
                ShowFilterRow = true;
                PageSize = 4;
                ShowGroupPanel = true;
                AllowSort = false;
                _initialParametersSet = true;
            }
            return base.SetParametersAsyncCore(parameters);
        }
    }
}