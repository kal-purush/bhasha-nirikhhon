namespace GitBitterLib
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Todo: Implement using https://github.com/octokit/octokit.net
    /// </summary>
    public class GitHubLister : IBitterRepositoryLister
    {
        private const string appName = "gitbitter:github";
        private const string preferedLinkProtocol = "https";
        private string username;

        /// <summary>
        /// Currently just for setting up login credentials
        /// </summary>
        /// <returns></returns>
        protected bool Login()
        {
            var cred = CredentialManager.ReadCredential(appName);
            while (cred == null)
            {
                var promptedcredentials = CredentialUI.PromptForCredentialsWithSecureString(appName, "GitBitter", "Please enter your GitHub login credentials");
                if (promptedcredentials != null)
                {
                    CredentialManager.WriteCredential(appName, promptedcredentials.UserName, promptedcredentials.Password);

                    cred = CredentialManager.ReadCredential(appName);
                }
                else
                {
                    return false;
                }
            }

            //sharpBucket.BasicAuthentication(cred.UserName, cred.Password.ToInsecureString());
            username = cred.UserName;
            cred = null;

            return true;
        }

        public List<RepositoryDescription> GetRepositories(string team)
        {
            Login();

            throw new NotImplementedException();
        }
    }
}