
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Catel.Runtime.Serialization.Xml;
using Catel.Services;
using HighFreqUpdate.Services.Interfaces;
using Infragistics.Windows.DataPresenter;

namespace HighFreqUpdate.Services
{
    public class GridPersistenceService : IGridPersistenceService
    {
        private IDispatcherService dispatcherService;
        private IXmlSerializer xmlSerializer;

        public GridPersistenceService(IDispatcherService dispatcherService,IXmlSerializer xmlSerializer)
        {
            this.dispatcherService = dispatcherService;
            this.xmlSerializer = xmlSerializer;
        }

        public Task PersistGrid(XamDataGrid grid)
        {
            //return dispatcherService.InvokeAsync(() =>
            //{
            //    using (MemoryStream memoryStream = new MemoryStream())
            //    {
            //        grid.SaveCustomizations(memoryStream);

            //        byte[] bytes = memoryStream.ToArray();

            //        var str = Encoding.UTF8.GetString(bytes);

            //        var externalInformations = GetGridExternalInformations(grid);

            //        IXmlSerializer x = ServiceLocator.Default.ResolveType<IXmlSerializer>();

            //        using (var ms = new MemoryStream())
            //        {
            //            x.Serialize(externalInformations, ms);

            //            byte[] bytes2 = ms.ToArray();

            //            var str1 = Encoding.UTF8.GetString(bytes2);

            //            File.WriteAllText(GridCustomizations, str1);
            //        }

            //        File.WriteAllText(GridLayout, str);

            //        timer.Start();
            //        dispatcherTimer.Start();
            //    }
            //});

            return null;
        }

        public Task RestoreGrid(XamDataGrid grid)
        {
            return null;
            //var str = File.ReadAllText(GridLayout);

            //byte[] bytes = Encoding.UTF8.GetBytes(str);

            //return dispatcherService.InvokeAsync(() =>
            //{
            //    using (var memoryStream = new MemoryStream(bytes))
            //    {
            //        grid.LoadCustomizations(memoryStream);
            //        timer.Start();
            //        dispatcherTimer.Start();
            //    }


            //    //grid customizations
            //    var customizations = File.ReadAllText(GridCustomizations);

            //    byte[] bytesCustomizations = Encoding.UTF8.GetBytes(customizations);


            //    using (var ms = new MemoryStream(bytesCustomizations))
            //    {
            //        IXmlSerializer x = ServiceLocator.Default.ResolveType<IXmlSerializer>();

            //        GridCustomizations c = x.Deserialize(typeof(GridCustomizations), ms) as GridCustomizations;

            //        LoadCustomGridSettings(grid, c);

            //    }
            //});
        }
    }
}