using System;
using System.Collections.Generic;
using System.Text;
using DAL.Interfaces;
using BusinessLogic.Models;
using BusinessLogic.Converter;
using System.Linq;
using BusinessLogic.Interfaces;

namespace BusinessLogic.Containers
{
    public class CommentContainer : ILogicCommentContainer
    {
        private ICommentContainer _Comments;
        private CommentConverter _CommentConverter;
        /// <summary>
        /// Depends on and expects given interfaces(IPageContainer) which realizes within the given DAL
        /// </summary>
        /// <param name="SQLCommentContext"></param>
        public CommentContainer(ICommentContainer SQLCommentContext, CommentConverter commentConverter)
        {
            _Comments = SQLCommentContext;
            _CommentConverter = commentConverter;
        }

        public List<CommentModel> GetallComments()
        {
            List<CommentModel> allText = _CommentConverter.Convert_To_CommentModel(_Comments.GetallComments());
            return allText;
        }


        public void CreateComment(CommentModel comment)
        {
            try
            {
                _Comments.CreateComment(_CommentConverter.Convert_To_DTO_CommentModel(comment));
            }
            catch (Exception)
            {

                throw;
            }
        }

        public void DeleteComment(int ID)
        {
            _Comments.DeleteComment(ID);
        }
    }
}