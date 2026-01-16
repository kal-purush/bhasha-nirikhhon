using ADOGenerator.Models;
using RestAPI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ADOGenerator.IServices
{
    public interface IExtractorService
    {
        ProjectConfigurations ProjectConfiguration(Project model);
        int GetTeamsCount(ProjectConfigurations appConfig);
        int GetIterationsCount(ProjectConfigurations appConfig);
        int GetBuildDefinitionCount(ProjectConfigurations appConfig);
        int GetReleaseDefinitionCount(ProjectConfigurations appConfig);
        string[] GenerateTemplateArifacts(Project model);
        Dictionary<string, int> GetWorkItemsCount(ProjectConfigurations appConfig);
        List<RequiredExtensions.ExtensionWithLink> GetInstalledExtensions(ProjectConfigurations appConfig);
        void ExportQuries(ProjectConfigurations appConfig);
        bool ExportTeams(RestAPI.ADOConfiguration con, Project model);
        bool ExportIterations(ProjectConfigurations appConfig);
        void ExportWorkItems(ProjectConfigurations appConfig);
        void ExportRepositoryList(ProjectConfigurations appConfig);
        int GetBuildDefinitions(ProjectConfigurations appConfig);
        int GeneralizingGetReleaseDefinitions(ProjectConfigurations appConfig);
        void GetServiceEndpoints(ProjectConfigurations appConfig);
        void ExportDeliveryPlans(ProjectConfigurations appConfig);
        
    }
}