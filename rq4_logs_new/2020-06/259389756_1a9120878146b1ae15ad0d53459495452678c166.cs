using Hippologamus.Server.Models;
using Microsoft.AspNetCore.Components;

namespace Hippologamus.Server.Components
{
    public class ErrorLogPagedListGridBase : ComponentBase
    {
        [Parameter]
        public ErrorLogPagedList ErrorLogPagedList { get; set; }
    }
}