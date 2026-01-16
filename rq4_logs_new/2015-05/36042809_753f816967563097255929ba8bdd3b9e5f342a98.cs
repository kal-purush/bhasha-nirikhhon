using System.Collections.Generic;
using System.IO;
using EnvDTE;
using Microsoft.VisualStudio.TemplateWizard;

namespace TemplateBuilder {
    public class MultiProjectRootWizard : WizardBase {
        private string _ProjectName;

        public override void RunStarted(object automationObject, Dictionary<string, string> replacementsDictionary, WizardRunKind runKind, object[] customParams) {
            GlobalDictionary["$saferootprojectname$"] = replacementsDictionary["$safeprojectname$"];
            _ProjectName = replacementsDictionary["$safeprojectname$"];
        }

        public override void ProjectFinishedGenerating(Project project) {
            if (GlobalProjects.Count < 1) {
                return;
            }

            var solution = GlobalProjects[0].DTE.Solution;
            string rougeDirectory = Path.Combine(new DirectoryInfo(Path.GetDirectoryName(solution.FullName)).FullName, _ProjectName);

            foreach (var item in GlobalProjects) {
                solution.Remove(item);
                var d = new DirectoryInfo(Path.GetDirectoryName(item.FullName));
                Directory.Move(d.FullName, d.Parent.Parent.FullName);
                string newPath = Path.Combine(d.Parent.Parent.FullName, d.Name, Path.GetFileName(item.FullName));
                solution.AddFromFile(newPath);
            }

            Directory.Delete(rougeDirectory);
        }
    }
}