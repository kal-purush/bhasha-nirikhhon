using ErrorOr;

namespace Rtl.TvMazeScrapper.Domain.Extensions.Errors;

public static partial class Errors
{
    public static class Show
    {
        public static Error GeneralErrorOccured => Error.Failure(
            code: "General.Failure",
            description: "An error occured!!"
        );
    }
}