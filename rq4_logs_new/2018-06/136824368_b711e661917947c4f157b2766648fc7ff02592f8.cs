namespace Pipelines.Responses
{
    public class Response<TSuccess, TError>
    {
        public TSuccess Success { get; }
        public TError Error { get; }
        public bool IsSuccess { get; set; }

        public Response(TSuccess success)
        {
            Success = success;
            IsSuccess = true;
        }

        public Response(TError error)
        {
            Error = error;
        }

        public static implicit operator Response<TSuccess, TError>(TSuccess success)
        {
            return new Response<TSuccess, TError>(success);
        }

        public static implicit operator Response<TSuccess, TError>(TError error)
        {
            return new Response<TSuccess, TError>(error);
        }
    }
}