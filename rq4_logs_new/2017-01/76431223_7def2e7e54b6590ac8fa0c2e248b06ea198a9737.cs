using Quiz.DAL;
using Quiz.Resolver;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Quiz.BLL.ModelServices;

namespace Quiz.BLL
{
    [Export(typeof(IComponent))]
    public class DependencyResolver : IComponent
    {
        public void SetUp(IRegisterComponent registerComponent)
        {
            registerComponent.RegisterType<IUnitOfWork, UnitOfWork>();
            registerComponent.RegisterType<IAnswerResultService, AnswerResultService>();
            registerComponent.RegisterType<ICategoryService, CategoryService>();
            registerComponent.RegisterType<IQuestionAnswerService, QuestionAnswerService>();
            registerComponent.RegisterType<IQuestionLevelService, QuestionLevelService>();
            registerComponent.RegisterType<IQuestionService, QuestionService>();
            registerComponent.RegisterType<IQuizOptionService, QuizOptionService>();
            registerComponent.RegisterType<IQuizSessionService, QuizSessionService>();
            registerComponent.RegisterType<IResultService, ResultService>();
            registerComponent.RegisterType<IUserService, UserService>();
        }
    }
}