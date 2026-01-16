namespace JobSeek.Api.Responses
{
    public class ListResponse<T>
    {
        public ResponseStatus Status { get; set; }
        public List<T>? Result { get; set; }
        public static ListResponse<T> Success(List<T> result)
        {
            return new ListResponse<T> { Status = ResponseStatus.Success, Result = result };
        }
        public static ListResponse<T> Failed(ResponseStatus status)
        {
            return new ListResponse<T> { Status = status, Result = null };
        }
    }
}