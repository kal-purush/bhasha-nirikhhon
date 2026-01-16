namespace TestWebAPI.DTOs
{
    using System.Collections.Generic;

    /// <summary>
    /// The request filter.
    /// </summary>
    public class EmployeeRequestFilter
    {
        /// <summary>
        /// Gets or sets the first name.
        /// </summary>
        public List<string> FirstName { get; set; }

        /// <summary>
        /// Gets or sets the last name.
        /// </summary>
        public List<string> LastName { get; set; }

        /// <summary>
        /// Gets or sets the email.
        /// </summary>
        public List<string> Email { get; set; }

        /// <summary>
        /// Gets or sets the title.
        /// </summary>
        public List<string> Title { get; set; }
    }
}