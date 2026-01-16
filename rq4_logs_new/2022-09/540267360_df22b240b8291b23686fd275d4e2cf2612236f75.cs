using System.Net;

namespace OEZZ.ERP.Application.Base;

public class Result : IResult
{
    public int Code { get; }
    public bool Success => Errors?.Any() ?? true;
    public string Message { get; }
    public IEnumerable<string>? Errors { get; }

    public Result(
        string message,
        int code,
        IEnumerable<string>? errors = default
    )
    {
        Message = message;
        Code = code;
        Errors = errors;
    }
}

public class Result<T> : Result, IResult<T>
{
    public T? Data { get; set; }

    public Result(
        string message,
        int code,
        T? data = default,
        IEnumerable<string>? errors = default
    ) : base(message, code, errors)
    {
        Data = data;
    }
}