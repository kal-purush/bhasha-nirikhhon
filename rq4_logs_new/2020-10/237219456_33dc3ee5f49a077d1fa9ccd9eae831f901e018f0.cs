using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "test project")]
[assembly: SuppressMessage("StyleCop.CSharp.ReadabilityRules", "SA1121:Use built-in type alias", Justification = "Source package. Will fix later.", Scope = "member", Target = "~M:NServiceBus.Gateway.AcceptanceTests.FaultyChannelSender`1.Send(System.String,System.Collections.Generic.IDictionary{System.String,System.String},System.IO.Stream)~System.Threading.Tasks.Task")]