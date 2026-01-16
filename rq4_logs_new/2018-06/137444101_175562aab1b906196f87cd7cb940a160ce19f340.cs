using P3.Konsole.Commands;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleCli.Commands
{
    public class NestedCommand : ICommand<string[]>
    {
        public Task ExecuteAsync(string[] parameter, CancellationToken token = default(CancellationToken)) {
            Console.WriteLine(this.GetType().FullName);
            Console.WriteLine(string.Join(" ", parameter));
            return Task.CompletedTask;
        }
    }
    public class NestedCommand2 : NestedCommand { }
    public class NestedCommand3 : NestedCommand { }
    public class NestedCommand4 : NestedCommand { }
    public class NestedCatchAllCommand : NestedCommand { }
}