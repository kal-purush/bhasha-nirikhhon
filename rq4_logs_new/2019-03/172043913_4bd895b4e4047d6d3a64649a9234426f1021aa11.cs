using Integratieproject1.BL.Models.Ideations;
using Integratieproject1.BL.Models.Projects;
using Integratieproject1.BL.Models.Surveys;

namespace Integratieproject1.BL.Managers
{
    public class ApplicationManager
    {
        #region Getters
         public Platform GetPlatform(int i)
                {
                    UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                    ProjectsManager projectsManager = new ProjectsManager(unitOfWorkManager);
                    Platform platform = projectsManager.GetPlatform(i);
                    return platform;
                }
        
                public Project GetProject(int projectId)
                {
                    UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                    ProjectsManager projectsManager = new ProjectsManager(unitOfWorkManager);
                    Project project = projectsManager.GetProject(projectId);
                    return project;
                }
        
                public Ideation GetIdeation(int ideationId)
                {
                    UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                                IdeationsManager ideationsManager = new IdeationsManager(unitOfWorkManager);
                                Ideation ideation = ideationsManager.GetIdeation(ideationId);
                                return ideation;
                }
        
                public Survey GetSurvey(int surveyId)
                {
                    UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                    SurveysManager surveysManager = new SurveysManager(unitOfWorkManager);
                    Survey survey = surveysManager.GetSurvey(surveyId);
                    return survey;
                }

                public Phase GetPhase(int phaseId)
                {
                    UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                    ProjectsManager projectsManager = new ProjectsManager(unitOfWorkManager);
                    Phase phase = projectsManager.GetPhase(phaseId);
                    return phase;
                }
            #endregion


            #region Setters

            

            #endregion

            #region Creators

            public void CreatePlatform(Platform platform)
            {
                UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                ProjectsManager projectsManager = new ProjectsManager(unitOfWorkManager);
                projectsManager.CreatePlatform(platform);
            }

            public void CreateProject(Project project)
            {
                UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                ProjectsManager projectsManager = new ProjectsManager(unitOfWorkManager);
                projectsManager.CreateProject(project);
            }

            public void CreatePhase(Phase phase)
            {
                UnitOfWorkManager unitOfWorkManager = new UnitOfWorkManager();
                ProjectsManager projectsManager = new ProjectsManager(unitOfWorkManager);
                projectsManager.CreatePhase(phase);
            }

            #endregion

            
    }
}