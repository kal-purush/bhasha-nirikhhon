using JQ.Extensions;
using JQ.Result;
using System.Text;
using System.Web.Mvc;
using System.Web.Routing;

namespace JQ.Web.Tool
{
    /// <summary>
    /// Copyright (C) 2015 备胎 版权所有。
    /// 类名：PageHelper.cs
    /// 类属性：公共类（非静态）
    /// 类功能描述：页面帮助类
    /// 创建标识：yjq 2017/7/10 11:47:36
    /// </summary>
    public static class PageHelper
    {
        public static MvcHtmlString SelfPager(this HtmlHelper htmlHelper, IPageResult pageResult)
        {
            int pageSize = pageResult.PageSize;
            int currentPage = pageResult.PageIndex;
            int totalCount = pageResult.TotalCount;
            int pageCount = pageResult.PageCount;
            int prevPage = currentPage - 1;
            prevPage = prevPage > 0 ? prevPage : 1;
            int nextPage = currentPage + 1;
            nextPage = nextPage > pageCount ? pageCount : nextPage;

            StringBuilder htmlBuilder = new StringBuilder();

            #region 页面链接

            htmlBuilder.Append("<div class='r dataTables_paginate mr-220'>");//页面链接
            if (pageCount >= 1)//显示页码
            {
                string routeUrl = htmlHelper.GetUrlFormat();

                htmlBuilder.Append(GetHrefInfo(1, "<<", currentPage == 1, routeUrl));//首页
                //上一页
                htmlBuilder.Append(GetHrefInfo(prevPage, "<", currentPage == 1, routeUrl));//上一页

                //中间页码条 前4后5
                int startPageIndex = currentPage - 4;
                int endPageIndex = currentPage + 5;
                if (startPageIndex > 1)
                {
                    htmlBuilder.Append(GetHrefInfo(1, "...", true, routeUrl));
                }
                for (int i = startPageIndex; i < endPageIndex; i++)
                {
                    if (i > 0 && i <= pageCount)
                    {
                        htmlBuilder.Append(GetHrefInfo(i, i.ToString(), i == currentPage, routeUrl, i == currentPage));
                    }
                }
                if (endPageIndex < pageCount)
                {
                    htmlBuilder.Append(GetHrefInfo(1, "...", true, routeUrl));
                }
                //下一页
                htmlBuilder.Append(GetHrefInfo(nextPage, ">", nextPage <= currentPage, routeUrl));//下一页
                //尾页
                htmlBuilder.Append(GetHrefInfo(pageCount, ">>", currentPage == pageCount, routeUrl));//尾页
            }
            htmlBuilder.Append("</div>");

            #endregion 页面链接

            return MvcHtmlString.Create(htmlBuilder.ToString());
        }

        /// <summary>
        /// 获取地址参数信息
        /// </summary>
        /// <param name="htmlHelper"></param>
        /// <returns>地址参数信息</returns>
        public static string GetUrlFormat(this HtmlHelper htmlHelper)
        {
            RouteValueDictionary routeValueDic;
            if (htmlHelper.ViewContext.RouteData.Values != null)
            {
                routeValueDic = new RouteValueDictionary(htmlHelper.ViewContext.RouteData.Values);
            }
            else
            {
                routeValueDic = new RouteValueDictionary();
            }
            string pageIndexUrlPara = "PageIndex";
            var queryUrlString = htmlHelper.ViewContext.HttpContext.Request.QueryString;
            if (queryUrlString.IsNotNull())
            {
                foreach (string urlParamKey in queryUrlString.Keys)
                {
                    if (urlParamKey.IsNotNullAndNotWhiteSpace())
                    {
                        routeValueDic[urlParamKey] = queryUrlString[urlParamKey];
                    }
                }
            }
            routeValueDic[pageIndexUrlPara] = "99919";//先给个默认页面，后面好批量替换
            UrlHelper urlHelper = new UrlHelper(htmlHelper.ViewContext.RequestContext);
            var routeUrl = urlHelper.RouteUrl(routeValueDic);
            return routeUrl.Replace("99919", "{0}"); ;
        }

        /// <summary>
        /// 获取链接信息
        /// </summary>
        /// <param name="pageIndex">当前页面id</param>
        /// <param name="desc">显示内容</param>
        /// <param name="isDisabled">是否无效(不能点击)</param>
        /// <param name="routeUrlFormat">路由信息</param>
        /// <returns>链接信息</returns>
        private static string GetHrefInfo(int pageIndex, string desc, bool isDisabled, string routeUrlFormat, bool isCurrent = false)
        {
            if (isDisabled)
            {
                return string.Format(" <a class='btn {0} radius disabled'>{1}</a>", isCurrent ? "btn-primary" : "btn-secondary-outline", desc);
            }
            else
            {
                string url = string.Format(routeUrlFormat, pageIndex.ToString());
                return string.Format("<a class='btn btn-secondary-outline radius' title='转到第{0}页' href='{1}'>{2}</a>", pageIndex.ToString(), url, desc);
            }
        }
    }
}