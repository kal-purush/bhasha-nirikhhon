using System;
using System.Collections.Concurrent;
using F1.Src.Common;
using F1.Src.Presentation;
using Microsoft.AspNetCore.Http;

namespace F1.Src.Mapper;

public static class F1HttpResponseMapper
{
    private static ConcurrentDictionary<int, Func<int, F1Response>> _httpResponseMapper;

    private static void Init()
    {
        if (Equals(_httpResponseMapper, null))
        {
            _httpResponseMapper = new();

            _httpResponseMapper.TryAdd(
                F1Constant.AppCode.PASSWORD_IS_INCORRECT,
                appCode => new F1Response
                {
                    HttpCode = StatusCodes.Status401Unauthorized,
                    AppCode = appCode,
                }
            );

            _httpResponseMapper.TryAdd(
                F1Constant.AppCode.TEMPORARY_BANNED,
                appCode => new F1Response
                {
                    HttpCode = StatusCodes.Status429TooManyRequests,
                    AppCode = appCode,
                }
            );

            _httpResponseMapper.TryAdd(
                F1Constant.AppCode.SUCCESS,
                appCode => new F1Response { HttpCode = StatusCodes.Status200OK, AppCode = appCode }
            );

            _httpResponseMapper.TryAdd(
                F1Constant.AppCode.USER_NOT_FOUND,
                appCode => new F1Response
                {
                    HttpCode = StatusCodes.Status404NotFound,
                    AppCode = appCode,
                }
            );

            _httpResponseMapper.TryAdd(
                F1Constant.AppCode.VALIDATION_FAILED,
                appCode => new F1Response
                {
                    HttpCode = StatusCodes.Status400BadRequest,
                    AppCode = appCode,
                }
            );
        }
    }

    public static F1Response Get(int appCode)
    {
        Init();

        return _httpResponseMapper[appCode](appCode);
    }
}