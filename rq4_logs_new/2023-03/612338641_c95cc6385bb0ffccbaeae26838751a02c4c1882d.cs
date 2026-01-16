using API.Dtos;
using API.Helpers;
using AutoMapper;
using Core.Entities;
using Core.Interfaces;
using Core.Specifications;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    public class ServicesController : BaseApiController
    {
        private readonly IMapper _mapper;
        private readonly IPhotoService _photoService;
        private readonly IGenericRepository<Service> _servicesRepo;
        private readonly IGenericRepository<Category> _categoriesRepo;
        private readonly IUnitOfWork _uow;
        public ServicesController(IGenericRepository<Service> servicesRepo, IGenericRepository<Category> categoriesRepo, IMapper mapper, IPhotoService photoService, IUnitOfWork uow)
        {
            _uow = uow;
            _categoriesRepo = categoriesRepo;
            _servicesRepo = servicesRepo;
            _photoService = photoService;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<ActionResult<Pagination<ServiceDto>>> GetServices([FromQuery] ServiceSpecParams specParams)
        {
            var spec = new ServicesSpecification(specParams);
            var countSpec = new ServicesWithFiltersForCountSpecification(specParams);

            var totalItems = await _servicesRepo.CountAsync(countSpec);
            var products = await _servicesRepo.ListAsync(spec);

            var data = _mapper.Map<IReadOnlyList<ServiceDto>>(products);

            return Ok(new Pagination<ServiceDto>(specParams.PageIndex, specParams.PageIndex, totalItems, data));
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ServiceDto>> GetService(int id)
        {
            var spec = new ServicesSpecification(id);

            var service = await _servicesRepo.GetEntityWithSpec(spec);

            if (service == null) return NotFound("This service might not exist");

            return Ok(_mapper.Map<Service, ServiceDto>(service));
        }

        [HttpPost]
        public async Task<ActionResult> Add(Service request)
        {
            _servicesRepo.Add(request);

            if (await _uow.Complete() < 0) return BadRequest();

            return Ok();
        }

        [HttpPut]
        public async Task<ActionResult> Update(ServiceUpdateDto request)
        {
            var service = await _servicesRepo.GetEntityWithSpec(new ServicesSpecification(request.Id));

            service.ServiceCategory = new ServiceCategory
            {
                CategoryId = request.ServiceCategory.CategoryId
            };

            _mapper.Map<ServiceUpdateDto, Service>(request, service);

            if (await _uow.Complete() < 0) return BadRequest("Failed to edit the service");

            return Ok();
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> Delete(int id)
        {
            var service = await _servicesRepo.GetEntityWithSpec(new ServicesSpecification(id));

            if (service == null) return NotFound("Service not found");

            _servicesRepo.Delete(service);

            if (await _uow.Complete() < 0) return BadRequest();

            return Ok();
        }

        [HttpGet("categories")]
        public async Task<ActionResult<IEnumerable<Category>>> GetCategories()
        {
            var categories = await _categoriesRepo.ListAllAsync();

            if (categories == null) return BadRequest("An error occurred loading the categories");

            return Ok(categories);
        }

        [HttpPost("photo/{id}")]
        public async Task<ActionResult<ServiceDto>> AddPhoto(IFormFile file, int id)
        {
            var service = await _servicesRepo.GetEntityWithSpec(new ServicesSpecification(id));

            if (service == null) return NotFound();

            var result = await _photoService.AddPhoto(file);

            if (result.Error != null) return BadRequest(result.Error.Message);
            
            service.ServicePhotos.Add(new ServicePhoto {
                Photo = new Photo {
                    Url = result.SecureUrl.AbsoluteUri
                }
            });

            if (await _uow.Complete() < 0) return BadRequest();
            
            return Ok(_mapper.Map<Service, ServiceDto>(service));
        }

        [HttpDelete("photo/delete")]
        public async Task<ActionResult<ServiceDto>> DeletePhoto(int photoId, int serviceId)
        {
            var service = await _servicesRepo.GetEntityWithSpec(new ServicesSpecification(serviceId));

            if (service == null) return NotFound();

            service.ServicePhotos.Remove(service.ServicePhotos.Find(x => x.PhotoId == photoId));

            if (await _uow.Complete() < 0) return BadRequest();
            
            return Ok(_mapper.Map<Service, ServiceDto>(service));
        }
    }
}