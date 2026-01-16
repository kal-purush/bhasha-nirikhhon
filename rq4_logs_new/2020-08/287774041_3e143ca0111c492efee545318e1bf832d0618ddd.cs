// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "CA1031:Keine allgemeinen Ausnahmetypen abfangen", Justification = "Exception will be handled by given handler.", Scope = "member", Target = "~M:Toolkit.Extensions.TaskExtensions.SafelyIgnoreResultAsync(System.Threading.Tasks.Task,Toolkit.Behaviour.IExceptionHandler)")]