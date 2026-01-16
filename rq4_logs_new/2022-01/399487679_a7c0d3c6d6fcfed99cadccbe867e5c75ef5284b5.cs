namespace Igtampe.Neco.API {

    /// <summary>Object that represents an error result from the API <br/><br/> 
    /// This is mostly here to make it easier for the frontend to determine if there was an error with their request when the default return is an object.
    /// </summary>

    /**Further explanation: 
     * 
     * Because of the way we parse returns as either .text() or .json(), having a usual return be an object but an error result be text can make it difficult for the body of the
     * result to be parsed correctly, even if we know there's been an error. So, to make my life a little easier, let's do this instead.
     * 
     * I'm sure there's a better way to do this but this is a relatively quick and easy fix. Plus we get to specify additional data:
     * This could probably be an anonymous type generated at error but may as well make a reusable object.
     ***/
    public class ErrorResult {

        private const int NOT_FOUND = 404;
        private const int UNAUTHORIZED = 401;
        private const int FORBIDDEN = 403;
        private const int BAD_REQUEST = 400;
        private const int SERVER_ERROR = 500;

        /// <summary>Code for this error</summary>
        public int Code { get; set; }

        /// <summary>Error flag set to true to make it easier for the frontend to realize there's been an error</summary>
        public bool Error { get; set; } = true;

        /// <summary>Reason for the error</summary>
        public string Reason { get; set; } = "";

        /// <summary>Field that is most likely responsible for this error</summary>
        public string Field { get; set; } = "";

        /// <summary>Generates a NotFound Error Result</summary>
        /// <param name="Reason">Reason for the error</param>
        /// <param name="Field">Field most likely responsible for the error</param>
        /// <returns></returns>
        public static ErrorResult NotFound(string Reason = "", string Field = "") => new() { Code = NOT_FOUND, Reason = Reason, Field = Field };

        /// <summary>Generates a Unauthorized Error Result</summary>
        /// <param name="Reason">Reason for the error</param>
        /// <param name="Field">Field most likely responsible for the error</param>
        /// <returns></returns>
        public static ErrorResult Unauthorized(string Reason = "", string Field = "") => new() { Code = UNAUTHORIZED, Reason = Reason, Field = Field };

        /// <summary>Generates a Forbidden Error Result</summary>
        /// <param name="Reason">Reason for the error</param>
        /// <param name="Field">Field most likely responsible for the error</param>
        /// <returns></returns>
        public static ErrorResult Forbidden(string Reason = "", string Field = "") => new() { Code = FORBIDDEN, Reason = Reason, Field = Field };

        /// <summary>Generates a Bad Request Error Result</summary>
        /// <param name="Reason">Reason for the error</param>
        /// <param name="Field">Field most likely responsible for the error</param>
        /// <returns></returns>
        public static ErrorResult BadRequest(string Reason = "", string Field = "") => new() { Code = BAD_REQUEST, Reason = Reason, Field = Field };

        /// <summary>Generates a Server Error Result</summary>
        /// <param name="Reason">Reason for the error</param>
        /// <param name="Field">Field most likely responsible for the error</param>
        /// <returns></returns>
        public static ErrorResult ServerError(string Reason = "", string Field = "") => new() { Code = SERVER_ERROR, Reason = Reason, Field = Field };

        /// <summary>Reusable error results</summary>
        public class Reusable {

            /// <summary>Error Result due to an invalid or expired session</summary>
            public static ErrorResult InvalidSession { get; } = Unauthorized("Invalid Session", "SessionID");
        
        }
    }
}