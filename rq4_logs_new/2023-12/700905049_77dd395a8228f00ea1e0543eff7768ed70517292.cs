using Driving_School.Api.ModelsRequest.Course;
using Driving_School.Api.ModelsRequest.Employee;
using Driving_School.Api.ModelsRequest.Lesson;
using Driving_School.Api.ModelsRequest.Person;
using Driving_School.Api.ModelsRequest.Place;
using Driving_School.Api.ModelsRequest.Transport;
using EmployeeTypesAPI = Driving_School.Api.Enums.EmployeeTypes;
using GSBTypesAPI = Driving_School.Api.Enums.GSBTypes;

namespace Driving_School.Api.Tests
{
    static internal class TestDataGenerator
    {
        static internal CourseRequest CourseRequest(Action<CourseRequest>? action = null)
        {
            var result = new CourseRequest
            {
                Id = Guid.NewGuid(),
                Name = $"Name{Guid.NewGuid():N}",
                Description = $"Description{Guid.NewGuid():N}",
                Duration = Random.Shared.Next(20, 60),
                Price = Random.Shared.Next(1000, 5000),
            };

            action?.Invoke(result);
            return result;
        }

        static internal CreateCourseRequest CreateCourseRequest(Action<CreateCourseRequest>? action = null)
        {
            var result = new CreateCourseRequest
            {
                Name = $"Name{Guid.NewGuid():N}",
                Description = $"Description{Guid.NewGuid():N}",
                Duration = Random.Shared.Next(20, 60),
                Price = Random.Shared.Next(1000, 5000),
            };

            action?.Invoke(result);
            return result;
        }

        static internal TransportRequest TransportRequest(Action<TransportRequest>? action = null)
        {
            var item = new TransportRequest
            {
                Id = Guid.NewGuid(),
                Name = $"Name{Guid.NewGuid():N}",
                Number = $"Number",
                GSBType = GSBTypesAPI.Mechanical,
            };

            action?.Invoke(item);
            return item;
        }

        static internal CreateTransportRequest CreateTransportRequest(Action<CreateTransportRequest>? action = null)
        {
            var item = new CreateTransportRequest
            {
                Name = $"Name{Guid.NewGuid():N}",
                Number = $"Number",
                GSBType = GSBTypesAPI.Mechanical,
            };

            action?.Invoke(item);
            return item;
        }

        static internal PersonRequest PersonRequest(Action<PersonRequest>? action = null)
        {
            var item = new PersonRequest
            {
                Id = Guid.NewGuid(),
                LastName = $"LastName{Guid.NewGuid():N}",
                FirstName = $"FirstName{Guid.NewGuid():N}",
                Patronymic = $"Patronymic{Guid.NewGuid():N}",
                DateOfBirthday = DateTime.UtcNow.AddYears(-20),
                Passport = $"Passport{Guid.NewGuid():N}",
                Phone = $"Phone{Guid.NewGuid():N}",
            };

            action?.Invoke(item);
            return item;
        }

        static internal CreatePersonRequest CreatePersonRequest(Action<CreatePersonRequest>? action = null)
        {
            var item = new CreatePersonRequest
            {
                LastName = $"LastName{Guid.NewGuid():N}",
                FirstName = $"FirstName{Guid.NewGuid():N}",
                Patronymic = $"Patronymic{Guid.NewGuid():N}",
                DateOfBirthday = DateTime.UtcNow.AddYears(-20),
                Passport = $"Passport{Guid.NewGuid():N}",
                Phone = $"Phone{Guid.NewGuid():N}",
            };

            action?.Invoke(item);
            return item;
        }

        static internal EmployeeRequest EmployeeRequest(Action<EmployeeRequest>? action = null)
        {
            var item = new EmployeeRequest
            {
                Id = Guid.NewGuid(),
                EmployeeType = EmployeeTypesAPI.Student,
                Email = $"Email@Email.Email",
                Experience = 0,
                Number = $"Number-Number",
                Person = Guid.NewGuid()
            };

            action?.Invoke(item);
            return item;
        }

        static internal CreateEmployeeRequest CreateEmployeeRequest(Action<CreateEmployeeRequest>? action = null)
        {
            var item = new CreateEmployeeRequest
            {
                EmployeeType = EmployeeTypesAPI.Student,
                Email = $"Email@Email.Email",
                Experience = 0,
                Number = $"Number-Number",
                Person = Guid.NewGuid()
            };

            action?.Invoke(item);
            return item;
        }

        static internal PlaceRequest PlaceRequest(Action<PlaceRequest>? action = null)
        {
            var item = new PlaceRequest
            {
                Id = Guid.NewGuid(),
                Name = $"Name{Guid.NewGuid():N}",
                Description = $"Description{Guid.NewGuid():N}",
                Address = $"Address{Guid.NewGuid():N}",
            };

            action?.Invoke(item);
            return item;
        }

        static internal CreatePlaceRequest CreatePlaceRequest(Action<CreatePlaceRequest>? action = null)
        {
            var item = new CreatePlaceRequest
            {
                Name = $"Name{Guid.NewGuid():N}",
                Description = $"Description{Guid.NewGuid():N}",
                Address = $"Address{Guid.NewGuid():N}",
            };

            action?.Invoke(item);
            return item;
        }

        static internal LessonRequest LessonRequest(Action<LessonRequest>? action = null)
        {
            var item = new LessonRequest
            {
                Id = Guid.NewGuid(),
                StartDate = DateTimeOffset.UtcNow,
                EndDate = DateTimeOffset.UtcNow.AddHours(2),
                Instructor = Guid.NewGuid(),
                Student = Guid.NewGuid(),
                Place = Guid.NewGuid(),
                Course = Guid.NewGuid(),
                Transport = Guid.NewGuid(),
            };

            action?.Invoke(item);
            return item;
        }

        static internal CreateLessonRequest CreateLessonRequest(Action<CreateLessonRequest>? action = null)
        {
            var item = new CreateLessonRequest
            {
                StartDate = DateTimeOffset.UtcNow,
                EndDate = DateTimeOffset.UtcNow.AddHours(2),
                Instructor = Guid.NewGuid(),
                Student = Guid.NewGuid(),
                Place = Guid.NewGuid(),
                Course = Guid.NewGuid(),
                Transport = Guid.NewGuid(),
            };

            action?.Invoke(item);
            return item;
        }
    }
}