using System.Collections.Generic;
using System.Linq;

namespace IATec.Shared.HttpClient.Dto
{
    public class BaseResponseDto
    {
        public bool Success { get; private set; }
        private readonly List<ErrorDto> _errors = new List<ErrorDto>();
        public IReadOnlyList<ErrorDto> Errors => _errors.AsReadOnly();

        internal void AddError(string message)
        {
            _errors.Add(new ErrorDto(message));
        }

        internal void AddError(int? statusCode, string message)
        {
            _errors.Add(new ErrorDto(message, statusCode));
        }
        
        internal void AddRangeError(int? statusCode, List<string> message)
        {
            var errorList = message
                .Select(messageError => new ErrorDto(messageError, statusCode))
                .ToList();
            
            _errors.AddRange(errorList);
        }

        internal void SetSuccess(bool success)
        {
            Success = success;
        }
    }
}