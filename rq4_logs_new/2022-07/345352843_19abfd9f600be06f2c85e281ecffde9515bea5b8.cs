using Contractor.CLI.Tools;
using Contractor.Core.Helpers;
using Contractor.Core.Options;

namespace Contractor.CLI
{
    internal class DomainAdditionOptionParser
    {
        public static DomainAdditionOptions ParseOptions(IContractorOptions options, string[] args)
        {
            var domainOptions = new DomainAdditionOptions(options);

            domainOptions.IsVerbose = ArgumentParser.HasArgument(args, "-v", "--verbose");

            domainOptions.Domain = args[2].UpperFirstChar();

            return domainOptions;
        }
    }
}