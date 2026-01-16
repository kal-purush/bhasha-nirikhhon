namespace JobSeek.Api.Responses
{
    public class SingleRespons<T>
    {
        public ResponsStatus Status { get; set; }
        public T? Result { get; set; }

        public static SingleRespons<T> Success(T result)
        {
            return new SingleRespons<T> { Status = ResponsStatus.Success, Result = result };
        }
        public static SingleRespons<T> Failed(ResponsStatus status)
        {
            return new SingleRespons<T> { Status = status, Result = default };
        }
    }
}