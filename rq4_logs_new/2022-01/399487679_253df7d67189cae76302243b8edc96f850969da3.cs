namespace Igtampe.Neco.API.Requests {

    /// <summary>Request to change a user's roles</summary>
    public class ChangeRoleRequest {

        /// <summary>Whether or not this user is an Administrator</summary>
        public bool IsAdmin { get; set; } = false;

        /// <summary>Whether or not this user is a government entity or a member of the government</summary>
        public bool IsGov { get; set; } = false;

        /// <summary>Whether or not this user is a member of the Salary Determination Committee</summary>
        public bool IsSDC { get; set; } = false;

        /// <summary>Whether or not this user is allowed to upload images to the database</summary>
        public bool IsUploader { get; set; } = false;

    }
}