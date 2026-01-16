using System;
using System.Collections.Generic;
using System.Linq;

namespace WebApi.Common
{
    public class Result<T>
    {
        internal Result(IEnumerable<string> errors)
        {
            if (errors is null || !errors.Any())
                throw new ArgumentException("Argument \"errors\" cannot be null or empty.");

            Succeeded = false;
            Errors = errors;
            Data = default;
        }

        internal Result(T data)
        {
            Data = data;
            Succeeded = true;
            Errors = Array.Empty<string>();
        }

        public bool Succeeded { get; }

        public bool Failed { get => !Succeeded; }

        public T Data { get; }

        public IEnumerable<string> Errors { get; }

#pragma warning disable CS0693 // Type parameter has the same name as the type parameter from outer type
        public static Result<T> Success<T>(T data = default)
#pragma warning restore CS0693 
        {
            return new Result<T>(data);
        }

        public static Result<T> Failure(IEnumerable<string> errors)
        {
            return new Result<T>(errors);
        }

        public static Result<T> Failure(string error)
        {
            return new Result<T>(new[] { error });
        }
    }

    public class Result : Result<object>
    {
        internal Result(IEnumerable<string> errors) : base(errors)
        {
        }

        private Result() : base((object)null)
        {
        }

        public static Result Success() => new Result();

        public new static Result Failure(IEnumerable<string> errors) => new Result(errors);

        public new static Result Failure(string error) => new Result(new[] { error });
    }
}