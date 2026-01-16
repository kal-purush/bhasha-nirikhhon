using System.IO;
using System.Threading.Tasks;
using Catel;
using Catel.Runtime.Serialization.Xml;
using Catel.Services;
using Infragistics.Windows.DockManager;

namespace IF.WPF.Infragistics.Persistence.Services.Interfaces
{
    public class DockManagerPersistenceService : IDockManagerPersistenceService
    {
        #region Variables
        private readonly IDispatcherService dispatcherService;
        private readonly IXmlSerializer xmlSerializer; 
        #endregion

        public DockManagerPersistenceService(IDispatcherService dispatcherService, IXmlSerializer xmlSerializer)
        {
            Argument.IsNotNull(()=> dispatcherService);
            Argument.IsNotNull(()=> xmlSerializer);

            this.dispatcherService = dispatcherService;
            this.xmlSerializer = xmlSerializer;
        }

        public Task PersistGrid(XamDockManager dockManager, Stream stream, bool closeStream = false)
        {
            return dispatcherService.InvokeAsync(() =>
            {
                stream.Position = 0L;
                dockManager.SaveLayout(stream);

                if (closeStream)
                {
                    stream.Flush();
                    stream.Close();
                    stream.Dispose();
                }
            });
        }

        public Task RestoreGrid(XamDockManager dockManager, Stream stream, bool closeStream = false)
        {
            stream.Position = 0L;

            return dispatcherService.InvokeAsync(() =>
                {
                    stream.Position = 0L;
                    dockManager.LoadLayout(stream);

                    if (closeStream)
                    {
                        stream.Flush();
                        stream.Close();
                        stream.Dispose();
                    }
                });
        }
    }
}