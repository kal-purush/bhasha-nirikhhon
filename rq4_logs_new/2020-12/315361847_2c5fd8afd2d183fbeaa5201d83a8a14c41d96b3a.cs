using System;
using System.Text;
using BookLib.Helpers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BookLib.Filter
{
    //异常处理filter
    public class JsonExceptionFilter : IExceptionFilter
    {
        public IWebHostEnvironment Environment { get; }
        public ILogger<Program> Logger;

        public JsonExceptionFilter(IWebHostEnvironment env, ILogger<Program> logger)
        {
            Environment = env;
            Logger = logger;
        }
        //当发生异常时调用
        public void OnException(ExceptionContext context)
        {
            var error = new ApiError();
            //根据是否生产环境在error.Message返回不同的内容
            if (Environment.IsDevelopment())
            {
                error.Message = context.Exception.Message;
                error.Detail = context.Exception.ToString();
            }
            else
            {
                error.Message = "Server error!";
                error.Detail = context.Exception.Message;
            }

            //设定请求上下文的结果是一个ObjectResult(error)，当发生异常时返回结果会是序列化后的ApiError对象
            context.Result = new ObjectResult(error)
            {
                StatusCode = StatusCodes.Status500InternalServerError
            };

            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Server exception: {context.Exception.Message}");
            sb.AppendLine(context.Exception.ToString());
            Logger.LogCritical(sb.ToString());

        }
    }
}