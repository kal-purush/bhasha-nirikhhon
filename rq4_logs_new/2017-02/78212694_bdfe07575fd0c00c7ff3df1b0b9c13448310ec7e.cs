using Microsoft.AspNetCore.Mvc;
using Pc.Information.Interface.IQuestionBll;
using Pc.Information.Model.QuestionInfo;
using System;
using Pc.Information.Model.BaseModel;
using System.Collections.Generic;
using Pc.Information.Utility.DataConvert;

namespace Pc.Information.Api.Controllers
{
    /// <summary>
    /// Question controller
    /// </summary>
    public class QuestionController : BaseController
    {
        /// <summary>
        /// Interface user info server.
        /// </summary>
        private IQuestionBll QuestionInfoBll { get; set; }

        /// <summary>
        /// Constructed function.
        /// </summary>
        /// <param name="userInfoBll"></param>
        public QuestionController(IQuestionBll userInfoBll)
        {
            QuestionInfoBll = userInfoBll;
        }

        /// <summary>
        /// Add new question info.
        /// </summary>
        /// <param name="newQuestionInfo"></param>
        /// <returns></returns>
        [HttpGet]
        [HttpPost]
        [Route("AddQuestionInfo")]
        public ApiResultModel<DataBaseModel> AddQuestionInfo(PiFQuestionInfoModel newQuestionInfo)
        {
            var baseDataModel = new DataBaseModel();
            if (string.IsNullOrEmpty(newQuestionInfo.PiFQuestionTitle) || string.IsNullOrEmpty(newQuestionInfo.PiFQuestionContent))
            {
                baseDataModel.StateCode = "1000";
                baseDataModel.StateDesc = "Request data is invalid.";
                return ResponseDataApi(baseDataModel);
            }
            newQuestionInfo.PiFCreateTime = DateTime.Now;
            var info = QuestionInfoBll.AddQuestionInfo(newQuestionInfo);
            if (info < 1)
            {
                baseDataModel.StateCode = "1001";
                baseDataModel.StateDesc = "Add faild.";
            }
            else
            {
                baseDataModel.StateCode = "0000";
                baseDataModel.StateDesc = "Add Success.";
            }
            return ResponseDataApi(baseDataModel);
        }

        /// <summary>
        /// Updata question info.
        /// </summary>
        /// <param name="newQuestionInfo"></param>
        /// <returns></returns>
        [HttpGet]
        [HttpPost]
        [Route("UpdateQuestionInfo")]
        public ApiResultModel<DataBaseModel> UpdateQuestionInfo(PiFQuestionInfoModel newQuestionInfo)
        {
            var baseDataModel = new DataBaseModel();
            if (string.IsNullOrEmpty(newQuestionInfo.PiFQuestionTitle) || string.IsNullOrEmpty(newQuestionInfo.PiFQuestionContent))
            {
                baseDataModel.StateCode = "1000";
                baseDataModel.StateDesc = "Request data is invalid.";
                return ResponseDataApi(baseDataModel);
            }
            var info = QuestionInfoBll.UpdateQuestionInfo(newQuestionInfo);
            if (info < 1)
            {
                baseDataModel.StateCode = "1001";
                baseDataModel.StateDesc = "Update faild.";
            }
            else
            {
                baseDataModel.StateCode = "0000";
                baseDataModel.StateDesc = "Update Success.";
            }
            return ResponseDataApi(baseDataModel);
        }

        /// <summary>
        /// search Qustion info.
        /// </summary>
        /// <param name="id">id</param>
        /// <param name="startTime">start time</param>
        /// <param name="endTime">end time</param>
        /// <param name="title">Fuzzy search word</param>
        /// <param name="pageIndex">page index</param>
        /// <param name="pageSize">pagesize</param>
        /// <returns></returns>
        [HttpGet]
        [HttpPost]
        [Route("SearchQustionInfo")]
        public ApiResultModel<List<PiFQuestionInfoModel>> SearchQustionInfo(long id = 0, string startTime = "1900-1-1", string endTime = "1900-1-1", string title = null, int pageIndex = 1, int pageSize = 10)
        {
            var tStartTime = DataTypeConvertHelper.ToDateTime(startTime);
            var tTendTime = DataTypeConvertHelper.ToDateTime(endTime);
            var dataList = QuestionInfoBll.SearchQustionInfo(id, tStartTime, tTendTime, title, pageIndex, pageSize);
            return ResponseDataApi(dataList);
        }
    }
}