using CourseGRPC;

namespace PaymentService.API.GrpcServices
{
    public class GetCourseInfoService
    {
        private readonly GetCourseByIdService.GetCourseByIdServiceClient service;
        public GetCourseInfoService(GetCourseByIdService.GetCourseByIdServiceClient _service)
        {
            service = _service;
        }
        public async Task<GetCourseIdResponse> SendCourseId(int courseId)
        {
            try
            {
                var courseIdRequest = new GetCourseIdRequest { CourseId = courseId };
                return await service.GetCourseIdAsync(courseIdRequest);

            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }
}