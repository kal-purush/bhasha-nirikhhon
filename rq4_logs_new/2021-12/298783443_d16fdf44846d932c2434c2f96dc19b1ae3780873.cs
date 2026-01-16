namespace OldPartitionsRemover.Entites
{
    internal class Result<T>
    {
        private readonly T _data;
        private readonly string _error;

        private Result(T data, string error)
        {
            _data = data;
            _error = error;
        }

        public bool IsOk(out T data, out string error)
        {
            data = _data;
            error = _error;
            return !IsError();
        }

        private bool IsError() => _error != null;

        public static Result<T> Ok(T data) => new(data, null);
        public static Result<T> Error(string error) => new(default, error);
    }
}