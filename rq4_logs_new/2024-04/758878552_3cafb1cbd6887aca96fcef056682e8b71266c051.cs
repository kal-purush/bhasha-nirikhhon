using Jared.Application.Dtos.TaskDtos;
using Jared.Application.Queries.TaskQueries;
using Jared.Presentation.ColumnDefinitions;

namespace Jared.Presentation.Pages;

public partial class Dashboard
{
    public TaskPageDto Model { get; set; } = new();
    public Query Query { get; set; } = new();

    protected override async Task OnInitializedAsync()
    {
        await sendPageQuery(Query);
    }

    private async Task sendPageQuery(Query query)
    {
        query.Filter!.Add("Status", "6");

        var result = await Mediator.Send(new TaskPageQuery(
            query.Page,
            query.PageSize,
            query.SortingProperty,
            query.SortingDirection,
            query.Filter));

        if (!result.Success)
        {
            return;
        }

        Model = result.Data;
    }
}