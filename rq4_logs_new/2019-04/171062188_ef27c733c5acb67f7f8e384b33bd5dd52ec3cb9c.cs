
// This file is used by Code Analysis to maintain SuppressMessage 
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given 
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage(
    "Design", 
    "CA1034:Nested types should not be visible", 
    Justification = "This project uses nested types to structure unit tests and share fixture setup.")]

[assembly: SuppressMessage(
    "Naming",
    "CA1707:Identifiers should not contain underscores", 
    Justification = "This project uses underscores to separate clauses of test names")]
