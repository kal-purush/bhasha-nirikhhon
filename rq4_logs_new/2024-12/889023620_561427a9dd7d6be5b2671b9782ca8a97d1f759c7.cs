namespace Domain
{
    public class OperationResult<T>
    {
        public bool IsSuccessfull { get; private set; }
        public string ErrorMessage { get; private set; }
        public T Data { get; private set; }

        public string Message { get; private set; }

        public OperationResult(bool isSuccessfull, string errorMessage, T data, string message)
        {
            IsSuccessfull = isSuccessfull;
            ErrorMessage = errorMessage ?? string.Empty;
            Data = data;
            Message = message;
        }

        public static OperationResult<T> Successfull(T data, string message = "Operation successfull!")
        {
            return new OperationResult<T>(true, null, data, message);
        }

        public static OperationResult<T> Failure(string errorMessage, string message = "Operation failed!")
        {
            return new OperationResult<T>(false, errorMessage, default, message);
        }


    }
}