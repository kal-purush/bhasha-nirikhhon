using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FotoSortierer_v2.Helper.Builder.Interfaces;
using FotoSortierer_v2.Services.Interfaces;
using FotoSortierer_v2.ViewModel;
using FotoSortierer_v2.ViewModel.Interfaces;
using Helper.Builder;
using Model.Interfaces;
using Repository.Interfaces;

namespace FotoSortierer_v2.Services
{
    public class CameraService : ICameraService
    {
        private readonly ICameraViewModelBuilder _cameraViewModelBuilder;

        public CameraService(ICameraViewModelBuilder cameraViewModelBuilder)
        {
            _cameraViewModelBuilder = cameraViewModelBuilder;
        }

        public IEnumerable<ICameraViewModel> GetCameras(IEnumerable<IPhotoViewModel> photoCollection)
        {
            //var cameras = photoCollection.Select(photo => _cameraViewModelBuilder.Build(photo.Camera)).Distinct();

            // Todo: check why program gets into break point and exit if I take the above solution.
            var modelBuilder = new CameraModelBuilder(); // just for test purposes.
            var cameras = new List<ICameraViewModel>
            {
                _cameraViewModelBuilder.Build(modelBuilder.Build("LG Electronics", "bla 1")),
                _cameraViewModelBuilder.Build(modelBuilder.Build("Sony", "bla 2")),
                _cameraViewModelBuilder.Build(modelBuilder.Build("Nikon", "bla 3")),
                _cameraViewModelBuilder.Build(modelBuilder.Build("Samsung", "bla 4")),
                _cameraViewModelBuilder.Build(modelBuilder.Build("Canon", "bla 5")),
            };
            return cameras;
        }
    }
}