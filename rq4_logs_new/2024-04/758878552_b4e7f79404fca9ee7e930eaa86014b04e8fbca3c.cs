using Microsoft.AspNetCore.Components;

namespace Jared.Presentation.Components.Advanced;

public partial class DataGridFilter
{
    [Parameter]
    public string Id { get; set; } = default!;
    [Parameter]
    public string? Filter { get; set; }
    [Parameter]
    public Type Type { get; set; } = default!;
    [Parameter]
    public EventCallback<string> OnFilterChange { get; set; }
    private string dateFormat = "dd/MM/yyyy";

    public string? Value
    {
        get => Filter;
        set
        {
            if (value != Filter)
            {
                Filter = value;
                OnFilterChange.InvokeAsync(Filter);
            }
        }
    }

    public DateTime DateFrom
    {
        get
        {
            if (string.IsNullOrEmpty(Filter!.Split('-')[0]))
            {
                return DateTime.Now;
            }
            else
            {
                return DateTime.Parse(Filter!.Split('-')[0]);
            }
        }
        set
        {
            if (value.ToString(dateFormat) != Filter!.Split('-')[0])
            {
                Filter = createDateFilter(value, DateTo);
                OnFilterChange.InvokeAsync(Filter);
            }
        }
    }

    public DateTime DateTo
    {
        get
        {
            if (string.IsNullOrEmpty(Filter!.Split('-')[1]))
            {
                return DateTime.Now;
            }
            else
            {
                return DateTime.Parse(Filter!.Split('-')[1]);
            }
        }
        set
        {
            if (value.ToString(dateFormat) != Filter!.Split('-')[1])
            {
                Filter = createDateFilter(DateFrom, value);
                OnFilterChange.InvokeAsync(Filter);
            }
        }
    }

    private string createDateFilter(DateTime? dateFrom, DateTime? dateTo)
    {
        string from = string.Empty;
        string to = string.Empty;

        if (dateFrom is null)
        {
            from = DateTime.MinValue.ToString(dateFormat);
        }
        else
        {
            from = ((DateTime)dateFrom).ToString(dateFormat);
        }
        if (dateTo is null)
        {
            to = DateTime.MaxValue.ToString(dateFormat);
        }
        else
        {
            to = ((DateTime)dateTo).ToString(dateFormat);
        }

        return from + "-" + to;
    }
}