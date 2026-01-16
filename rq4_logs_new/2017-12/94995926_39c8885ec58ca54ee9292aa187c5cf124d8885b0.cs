using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Lykke.Common.Api.Contract.Responses;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace Lykke.Common.ApiLibrary.Contract
{
    /// <summary>
    /// Factory for the <see cref="ErrorResponse"/>
    /// </summary>
    [PublicAPI]
    public static class ErrorResponseFactory
    {
        /// <summary>
        /// Creates <see cref="ErrorResponse"/> from the <see cref="ModelStateDictionary"/>
        /// </summary>
        public static ErrorResponse Create(ModelStateDictionary modelState)
        {
            var response = new ErrorResponse
            {
                ModelErrors = new Dictionary<string, List<string>>()
            };

            foreach (var state in modelState)
            {
                var messages = state.Value.Errors
                    .Where(e => !string.IsNullOrWhiteSpace(e.ErrorMessage))
                    .Select(e => e.ErrorMessage)
                    .Concat(state.Value.Errors
                        .Where(e => string.IsNullOrWhiteSpace(e.ErrorMessage))
                        .Select(e => e.Exception.Message))
                    .ToList();

                if (messages.Any())
                {
                    response.ModelErrors.Add(state.Key, messages);
                }
            }

            return response;
        }
    }
}