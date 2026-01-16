namespace OctoStyle.Console
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    using NDesk.Options;

    public class Arguments
    {
        private Arguments(string solutionDirectory, string repositoryOwner, string repository, int pullRequestNumber)
        {
            if (solutionDirectory == null)
            {
                throw new ArgumentNullException("solutionDirectory");
            }

            if (repositoryOwner == null)
            {
                throw new ArgumentNullException("repositoryOwner");
            }

            if (repository == null)
            {
                throw new ArgumentNullException("repository");
            }

            const string cannotBeEmptyMessage = "Cannot be empty";

            if (solutionDirectory == String.Empty)
            {
                throw new ArgumentException(cannotBeEmptyMessage, "solutionDirectory");
            }

            if (repositoryOwner == String.Empty)
            {
                throw new ArgumentException(cannotBeEmptyMessage, "repositoryOwner");
            }

            if (repository == String.Empty)
            {
                throw new ArgumentException(cannotBeEmptyMessage, "repository");
            }

            this.SolutionDirectory = solutionDirectory;
            this.RepositoryOwner = repositoryOwner;
            this.Repository = repository;
            this.PullRequestNumber = pullRequestNumber;
        }

        public string SolutionDirectory { get; private set; }
        public string RepositoryOwner { get; private set; }
        public string Repository { get; private set; }
        public int PullRequestNumber { get; private set; }

        public static Arguments Parse(IEnumerable<string> args)
        {
            var solutionDirectory = default(string);
            var repositoryOwner = default(string);
            var repository = default(string);
            var pullRequestNumber = default(int);

            var options =new OptionSet()
                .Add("d=", d => solutionDirectory = d)
                .Add("o=", o => repositoryOwner = o)
                .Add("r=", r => repository = r)
                .Add("pr=",
                    pu =>
                        {
                            if (!int.TryParse(
                                pu,
                                NumberStyles.Integer,
                                CultureInfo.InvariantCulture,
                                out pullRequestNumber))
                            {
                                throw new ArgumentException("pu must be an integer referencing an active pull request");
                            }
                        });
            var res = options.Parse(args);

            try
            {
                return new Arguments(solutionDirectory, repositoryOwner, repository, pullRequestNumber);
            }
            catch (ArgumentException ex)
            {
                var helpMessage = String.Format(
                    CultureInfo.InvariantCulture,
                    "{1}{0}{2}{0}{3}{0}{4}",
                    Environment.NewLine,
                    "-d {Solution Directory",
                    "-o {Repository Owner}",
                    "-r {Repository}",
                    "-pr {Pull Request Number}");

                throw new ArgumentException(helpMessage, ex);
            }
        }
    }
}