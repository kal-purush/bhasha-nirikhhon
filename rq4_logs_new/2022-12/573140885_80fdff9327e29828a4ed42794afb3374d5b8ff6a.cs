

using System.Xml;
using System.Reflection;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

using Microsoft.OpenApi.Models;

using CommandLine;
using Jobs;
using Models;

public partial class Program
{
    public class ArgOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }
    }

    private static async Task Main(string[] args)
    {
        if (!validateArgs(args))
        {
            return;
        }

        ArgOptions? options = null;
        IEnumerable<CommandLine.Error>? errors = null;

        Parser.Default.ParseArguments<ArgOptions>(args)
            .WithParsed(opts => options = opts)
            .WithNotParsed((errs) => errors = errs);

        if (options is null)
        {
            HandleParseError(errors);
            return;
        }
        
        await RunOptionsAndReturnExitCode(options);
    }

    private static bool validateArgs(string[] args)
    {
        foreach (var arg in args)
        {
            if (!Regex.IsMatch(arg, @"^-?[a-zA-Z0-9]{1,40}$"))
            {
                return false;
            }
        }

        return true;
    }

    private static void HandleParseError(IEnumerable<CommandLine.Error>? errs)
    {
        if (errs is not null)
        {
            foreach (var err in errs)
            {
                Console.WriteLine(err.Tag.ToString());
            }
        }
    }

    private static async Task RunOptionsAndReturnExitCode(ArgOptions options)
    {
        string partnerId = "#0001";

        // TODO: 1) Authentication (JWT)

        // 2) Get Partner Data
        Partner? partner = await DBHandler.GetPartnerData(partnerId);
        if (partner is null)
        {
            return;
        }

        // 3) Start Jobs
        Producer producer = new(partner);
        Consumer consumer = new(partner);

        // 4) Produce Payload
        List<(XmlDocument, string)> payloads = await producer.GetPayloadsToProcess();

        // 5) Process Payload
        foreach (var item in payloads)
        {
            (XmlDocument payload, string payloadId) = item;

            var (response, statusCode) = await consumer.Run(payloadId, payload);

            // 6) Save Response
            // if (statusCode is not HttpStatusCode.OK)
            // {

            // }

            // XmlDocument xmlResponse = new() { PreserveWhitespace = true };
            // xmlResponse.Load(response);
            // InvoiceResponse result = XmlHandler.DeserializeResponse<InvoiceResponse>(xmlResponse);
        }
    }
}