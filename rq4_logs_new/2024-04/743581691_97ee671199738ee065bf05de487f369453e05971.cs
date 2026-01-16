
using Grpc.Core;
using Microsoft.EntityFrameworkCore;

namespace CourseGRPC.Services
{
    public class CheckCourseIdServicesGrpc
    {
        private readonly CheckCourseIdService.CheckCourseIdServiceClient service;
        public CheckCourseIdServicesGrpc(CheckCourseIdService.CheckCourseIdServiceClient _service)
        {
            service = _service;
        }
        public  async Task<CheckCourseIdResponse> SendCourseId(int courseId)
        {
            try
            {
                var courseIdRequest = new CheckCourseIdRequest { CourseId= courseId };
                return await service.CheckCourseIdAsync(courseIdRequest);

            }
            catch (Exception ex)
            {
                throw;
            }
        }

    }
}