using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;

namespace BobToolsCli
{
    public static class CliHelper
    {
        private static readonly Parser s_parser = Parser.Default;

        public static async Task RunWithParsed<TArgs>(string[] args, Func<TArgs, CancellationToken, Task> proc)
            => await WithToken(async t => await s_parser
                .ParseArguments<TArgs>(args)
                .MapResult(a => proc(a, t), ProcessErrors));

        public static async Task RunWithParsed<TArgs1, TArgs2>(string[] args, Func<TArgs1, CancellationToken, Task> proc1, Func<TArgs2, CancellationToken, Task> proc2)
            => await WithToken(async t => await s_parser
                .ParseArguments<TArgs1, TArgs2>(args)
                .MapResult<TArgs1, TArgs2, Task>(a => proc1(a, t), a => proc2(a, t), ProcessErrors));

        private static async Task WithToken(Func<CancellationToken, Task> f)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                cancellationTokenSource.Cancel();
            };

            try
            {
                await f(cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Cancelled");
            }
        }

        private static Task ProcessErrors(IEnumerable<Error> errs)
        {
            Console.WriteLine($"Errors: {string.Join(", ", errs)}");
            return Task.CompletedTask;
        }
    }
}