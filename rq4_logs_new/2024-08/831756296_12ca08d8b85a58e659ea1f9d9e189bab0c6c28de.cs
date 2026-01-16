namespace Domain.Shared
{
    public class UnauthorizedErrorModel
    {
        public string Error { get; set; }

        public UnauthorizedErrorModel()
        {
            Error = "response status is 401";
        }
    }
}