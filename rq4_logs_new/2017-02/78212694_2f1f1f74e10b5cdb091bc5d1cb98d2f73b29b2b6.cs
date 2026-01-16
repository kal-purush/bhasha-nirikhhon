using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Pc.Information.Interface.IQuestionReplyBll;
using Pc.Information.Model.BaseModel;
using Pc.Information.Model.QuestionInfo;

namespace Pc.Information.Api.Controllers
{
    /// <summary>
    /// Question reply info
    /// </summary>
    public class QuestionReplyController : BaseController
    {
        /// <summary>
        /// Interface Question reply server.
        /// </summary>
        private IQuestionReplyBll QuestionReplyInfoBll { get; set; }

        /// <summary>
        /// Constructed function.
        /// </summary>
        /// <param name="userInfoBll"></param>
        public QuestionReplyController(IQuestionReplyBll userInfoBll)
        {
            QuestionReplyInfoBll = userInfoBll;
        }

        /// <summary>
        /// AddQuestionReplyInfo
        /// </summary>
        /// <param name="questionId">questionId</param>
        /// <param name="userId">userId</param>
        /// <param name="content">content</param>
        /// <returns></returns>
        [Route("AddQuestionReplyInfo")]
        [HttpGet]
        [HttpPost]
        public ApiResultModel<DataBaseModel> AddQuestionReplyInfo(int questionId, int userId, string content)
        {
            PiFQuestionReplyInfoModel newQuestionReplyInfo = new PiFQuestionReplyInfoModel { PiFQuestionId = questionId, PiFReplyContent = content, PiFReplyIsBest = 0, PiFReplyTime = DateTime.Now, PiFReplyUserId = userId };
            var baseDataModel = new DataBaseModel();
            if (string.IsNullOrEmpty(newQuestionReplyInfo.PiFReplyContent) || newQuestionReplyInfo.PiFQuestionId < 1 || newQuestionReplyInfo.PiFReplyUserId < 1)
            {
                baseDataModel.StateCode = "1000";
                baseDataModel.StateDesc = "Request data is invalid.";
                return ResponseDataApi(baseDataModel);
            }
            var info = QuestionReplyInfoBll.AddQuestionReplyInfo(newQuestionReplyInfo);
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
    }
}