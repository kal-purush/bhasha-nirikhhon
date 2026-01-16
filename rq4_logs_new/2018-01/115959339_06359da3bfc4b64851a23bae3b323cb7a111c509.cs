using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Server.Adapters.Http;
using static HttpStatusCode;

internal class HttpStatusCode
{
    public static HttpStatusCode Ok = new HttpStatusCode(200, "OK");
    public static HttpStatusCode BadRequest = new HttpStatusCode(400, "Bad Request");
    public static HttpStatusCode NotFound = new HttpStatusCode(404, "Not Found");
    public static HttpStatusCode ServerError = new HttpStatusCode(500, "Server Error");

    public int Code { get; private set; }
    public string Description { get; private set; }

    private HttpStatusCode(int status, string description)
    {
        Code = status;
        Description = description;
    }

    public static HttpStatusCode From(int status)
    {
        var statusCode = typeof(HttpStatusCode)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(HttpStatusCode))
            .Select(f => (HttpStatusCode)f.GetValue(null))
            .FirstOrDefault(x => x.Code == status);

        if (statusCode == null)
            throw new ArgumentException(nameof(status),
                $"Status {status} is not supported.");

        return statusCode;
    }
}

internal interface IHttpResponse
{
    HttpStatusCode Status { get; }
    IEnumerable<MessageHeader> Headers { get; }
    byte[] Body { get; }
}

internal class HttpResponse : IHttpResponse
{
    private ICollection<MessageHeader> _headers;
    private ICollection<byte> _body;

    public HttpStatusCode Status { get; private set; }

    public IEnumerable<MessageHeader> Headers => _headers;

    public byte[] Body => _body.ToArray();

    internal HttpResponse(HttpStatusCode status)
    {
        Status = status;
        _headers = new List<MessageHeader>();
        _body = new List<byte>();
    }

    public void SetContent(string content)
    {
        _body = Encoding.UTF8.GetBytes(content);
        AddHeader("Content-Type", "text/html; charset=UTF-8");
        AddHeader("Content-Length", _body.Count().ToString());
    }

    public void AddHeader(string field, string value)
    {
        _headers.Add(MessageHeader.From(field, value));
    }
}

internal abstract class Resource
{
    protected abstract (int status, string content) Get(IHttpRequest request);
    protected abstract (int status, string content) Post(IHttpRequest request);
    protected abstract (int status, string content) Put(IHttpRequest request);
    protected abstract (int status, string content) Delete(IHttpRequest request);

    internal IHttpResponse GetResponse(IHttpRequest request)
    {
        if (request == null)
            return new HttpResponse(BadRequest);

        switch (request.Verb)
        {
            case HttpVerb.Get:
                return Make(Get(request));
            case HttpVerb.Post:
                return Make(Post(request));
            case HttpVerb.Put:
                return Make(Put(request));
            case HttpVerb.Delete:
                return Make(Delete(request));
            default:
                return new HttpResponse(BadRequest);
        }
    }

    private IHttpResponse Make((int status, string content) result)
    {
        var response = new HttpResponse(HttpStatusCode.From(result.status));
        response.SetContent(result.content);

        return response;
    }
}

internal class NullResource : Resource
{
    protected override (int status, string content) Delete(IHttpRequest request)
    => MakeNotFound;

    protected override (int status, string content) Get(IHttpRequest request)
    => MakeNotFound;

    protected override (int status, string content) Post(IHttpRequest request)
    => MakeNotFound;

    protected override (int status, string content) Put(IHttpRequest request)
    => MakeNotFound;

    private (int, string) MakeNotFound => (404, "");
}

internal interface IHttpRouteTable
{
    void Add(string relativePath, Resource resource);
    void Remove(string relativePath);
    Resource Find(string relativePath);
}

internal class HttpRouteTable : IHttpRouteTable
{
    private IDictionary<string, Resource> _routeTable;
    private Resource NullResouce { get; set; }

    public HttpRouteTable()
    {
        _routeTable = new Dictionary<string, Resource>();
        NullResouce = new NullResource();
    }

    public void Add(string relativePath, Resource resource)
    {
        if (!relativePath.StartsWith("/"))
            throw new ArgumentException(nameof(relativePath),
                $"Must be a relative path that starts with '/'");

        var (pathParameters, normalizedPath) = NormalizePath(relativePath);

        if (_routeTable.ContainsKey(normalizedPath))
            _routeTable[normalizedPath] = resource;

        _routeTable.Add(normalizedPath, resource);
    }

    public Resource Find(string relativePath)
    {
        if (string.IsNullOrEmpty(relativePath))
            return NullResouce;

        var nPath = NormalizePath(relativePath);

        return _routeTable.TryGetValue(relativePath, out Resource resource)
            ? resource
            : NullResouce;
    }

    private (ICollection<string> pathParameters, string path)
    NormalizePath(string relativePath)
    {
        var trimmedPath = relativePath.Trim();

        if (trimmedPath == "*")
            return (new string[] { }, trimmedPath);

        var parts = trimmedPath
            .Replace('\\', '/')
            .Split("/");

        if (parts.Length == 0)
            return (new string[] { }, trimmedPath);

        if (!parts.Any(p => p.StartsWith(":")))
            return (new string[] { }, trimmedPath);

        var pathParams = new List<string>();
        var sb = new StringBuilder();
        foreach (var p in parts)
        {
            if (p.StartsWith(":"))
            {
                pathParams.Add(p);
                sb.Append("/*");
                continue;
            }

            sb.Append($"/{p}");
        }

        return (pathParams, sb.ToString());
    }

    public void Remove(string relativePath)
    {
        throw new NotImplementedException();
    }
}