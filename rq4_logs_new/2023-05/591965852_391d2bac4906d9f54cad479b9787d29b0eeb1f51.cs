namespace JobSeek.Api.Responses
{
    public class SingleResponse<T>
    {
        public ResponseStatus Status { get; set; }
        public T? Result { get; set; }

        public static SingleResponse<T> Success(T result)
        {
            return new SingleResponse<T> { Status = ResponseStatus.Success, Result = result };
        }
        public static SingleResponse<T> Failed(ResponseStatus status)
        {
            return new SingleResponse<T> { Status = status, Result = default };
        }
    }
}