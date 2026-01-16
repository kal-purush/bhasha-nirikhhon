using System.Net;

namespace LetsPave.Core.Models
{
    public interface IBaseResponse<T>
    {
        bool IsSuccessful { get; }
        HttpStatusCode Status { get; set; }
        T UniqueId { get; set; }

        string Message { get; set; }

        IBaseResponse<T> Created(T t);

        IBaseResponse<T> ServerError(Exception x);
        IBaseResponse<T> Updated();
        IBaseResponse<T> NotFound();
        IBaseResponse<T> Deleted();
        BaseResponse<T> Ok(string messages, T Id);
        IBaseResponse<T> BadRequest(string message);
    }

    public class BaseResponse<T> : IBaseResponse<T>
    {
        public bool IsSuccessful =>
            Status == HttpStatusCode.OK ||
            Status == HttpStatusCode.Accepted ||
            Status == HttpStatusCode.Created ||
            Status == HttpStatusCode.Found;
        public HttpStatusCode Status { get; set; }
        public T UniqueId { get; set; }

        public string Message { get; set; }

        public IBaseResponse<T> Created(T id)
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.Created,
                Message = "Record created successfully",
                UniqueId = id
            };
        }

        public IBaseResponse<T> Updated()
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.OK,
                Message = "Record updated successfully",
            };
        }

        public BaseResponse<T> Ok(string messages, T uniqueId)
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.OK,
                Message = messages,
                UniqueId = uniqueId
            };
        }

        public IBaseResponse<T> ServerError(Exception x)
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.InternalServerError,
                Message = "Some error occurred while adding the record.",
            };
        }
        public IBaseResponse<T> NotFound()
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.NotFound,
                Message = "Record could not be found for given Id.",
            };
        }

        public IBaseResponse<T> NotFound(string message)
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.NotFound,
                Message = message,
            };
        }

        public IBaseResponse<T> Deleted()
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.OK,
                Message = "Record deleted successfully.",
            };
        }

        public IBaseResponse<T> BadRequest(string message)
        {
            return new BaseResponse<T>()
            {
                Status = HttpStatusCode.BadRequest,
                Message = message,
            };
        }

    }
}