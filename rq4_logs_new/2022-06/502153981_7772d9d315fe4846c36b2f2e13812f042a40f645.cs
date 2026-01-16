using System;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;

namespace PikTestPlugin
{
    public class Command : IExternalCommand
    {
        public Result Execute(ExternalCommandData commandData, ref string message, ElementSet elements)
        {
            try
            {
                //TODO: Вынести инициализацию в класс RevitStaticData
                RevitStaticData.Document = commandData.Application.ActiveUIDocument.Document;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return Result.Failed;
            }

            return Result.Succeeded;
        }
    }
}