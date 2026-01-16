using System.Collections;
using Newtonsoft.Json;

namespace OnlineStore.Domain.Common.Responses
{
    public class JsonResponse
    {
        [JsonProperty("success")]
        public bool Success { get; set; } = true;

        [JsonProperty("message")]
        public string Message { get; set; }

        [JsonProperty("data_type")]
        public string DataType
        {
            get
            {
                if (Data == null)
                {
                    return "";
                }
                else if (Data is int)
                {
                    return "integer";
                }
                else if (Data is bool)
                {
                    return "boolean";
                }
                else if (Data is IEnumerable)
                {
                    return "array";
                }
                else if (Data is object && !(Data is string))
                {
                    return "object";
                }
                else
                {
                    return "string";
                }
            }
        }

        [JsonProperty("data")]
        public object Data { get; set; }

        [JsonProperty("code")]
        public string Code { get; set; }

        public static JsonResponse SuccessResponseWithCode(string code, object data = null, string message = null)
        {
            var response = new JsonResponse() { Success = true, Message = message, Data = data, Code = string.IsNullOrEmpty(code) ? "success" : code.ToLower().StartsWith("success-") ? code.ToLower() : "success-" + code.ToLower() };
            if ((response.DataType == "array" || response.DataType == "json-array") && data == null)
            {
                response.Success = false;
                response.Code = "error";
            }
            return response;
        }

        public static JsonResponse SuccessResponse(string message = null, object data = null, string code = null)
        {
            var response = new JsonResponse() { Success = true, Message = message, Data = data, Code = string.IsNullOrEmpty(code) ? "success" : code.ToLower().StartsWith("success-") ? code.ToLower() : "success-" + code.ToLower() };
            if ((response.DataType == "array" || response.DataType == "json-array") && data == null)
            {
                response.Success = false;
                response.Code = "error";
            }
            else
            {
                response.Success = true;
                response.Code = "success";
            }
            return response;
        }

        public static JsonResponse ErrorResponseWithCode(string code, string message = null, object data = null)
        {
            return new JsonResponse() { Success = false, Message = message, Data = data, Code = string.IsNullOrEmpty(code) ? "error" : code.ToLower().StartsWith("error-") ? code.ToLower() : "error-" + code.ToLower() };
        }

        public static JsonResponse ErrorResponse(string message = null, object data = null, string code = null)
        {
            return new JsonResponse() { Success = false, Message = message, Data = data, Code = string.IsNullOrEmpty(code) ? "error" : code.ToLower().StartsWith("error-") ? code.ToLower() : "error-" + code.ToLower() };
        }

        public static JsonResponse DataResponse(object model, string message = null)
        {
            var response = new JsonResponse() { Success = model != null, Data = model, Message = message, Code = model != null ? "success" : "error" };
            if ((response.DataType == "array" || response.DataType == "json-array") && model == null)
            {
                response.Success = false;
                response.Code = "error";
            }
            else
            {
                response.Success = true;
                response.Code = "success";
            }
            return response;
        }

        public static JsonResponse ErrorResponse(Exception ex = null)
        {
            if (ex is ErrorCodeException)
                return new JsonResponse() { Success = false, Message = "error", Code = ex.Message };
            return new JsonResponse() { Success = false, Message = ex != null && !string.IsNullOrEmpty(ex.Message) ? ex.Message : "Произошла ошибка, пожалуйста, свяжитесь с администратором.", Code = "error" };
        }

        public static implicit operator JsonResponse(Exception ex)
        {
            if (ex is ErrorCodeException)
                return new JsonResponse() { Success = false, Message = "error", Code = ex.Message };
            return new JsonResponse() { Success = false, Message = ex != null && !string.IsNullOrEmpty(ex.Message) ? ex.Message : "Произошла ошибка, пожалуйста, свяжитесь с администратором.", Code = "error" };
        }
    }
}