using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace MediatR.ValidationGenerator.Gen
{
    public static class DiagnosticDescriptors
    {
        private static DiagnosticDescriptor _failedToCreateValidatorDescriptor = new DiagnosticDescriptor("VG0001",
          "Failed to create validator",
          "Failed to create validator for '{0}' request with message '{1}'",
          "Errors",
          DiagnosticSeverity.Error,
          true);

        public static DiagnosticDescriptor FailedToCreateValidatorDescriptor => _failedToCreateValidatorDescriptor;
    }
}