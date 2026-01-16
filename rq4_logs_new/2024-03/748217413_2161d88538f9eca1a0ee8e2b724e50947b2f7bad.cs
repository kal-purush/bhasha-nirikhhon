using Asp.Versioning;
using AutoMapper;
using FluentValidation;
using FluentValidation.Results;
using Map.API.Extension;
using Map.Domain.Entities;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.Testimonial;
using Map.Platform.Interfaces;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using static Map.API.Controllers.Models.HttpError;

namespace Map.API.Controllers;

//[Authorize(Roles = Roles.User)]
[ApiController]
[ApiVersion(ApiControllerVersions.V1)]
[Route("api/v{version:apiVersion}/[controller]")]
public class TestimonialController : ControllerBase
{
    #region props
    private readonly IMapper _mapper;
    private readonly IValidator<AddTestimonialDto> _addTestimonialValidator;
    private readonly UserManager<MapUser> _userManager;
    private readonly ITestimonialPlatform _testimonialPlatform;
    #endregion

    #region Ctor
    public TestimonialController(IMapper mapper, IValidator<AddTestimonialDto> addTestimonialValidator, UserManager<MapUser> userManager, ITestimonialPlatform testimonialPlatform)
    {
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _addTestimonialValidator = addTestimonialValidator ?? throw new ArgumentNullException(nameof(addTestimonialValidator));
        _userManager = userManager ?? throw new ArgumentNullException(nameof(userManager));
        _testimonialPlatform = testimonialPlatform ?? throw new ArgumentNullException(nameof(testimonialPlatform));
    }
    #endregion

    /// <summary>
    /// Add testimonial to user
    /// </summary>
    /// <param name="userId">UserId to add Testimonail</param>
    /// <param name="dto">AddTestimonialDto</param>
    [HttpPost]
    [Route("{userId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(TestimonialDto), StatusCodes.Status201Created)]
    [ProducesResponseType(typeof(ICollection<Error>), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<TestimonialDto>> CreateTestimonialAsync([FromRoute] Guid userId, [FromBody] AddTestimonialDto dto)
    {
        ValidationResult validationResult = _addTestimonialValidator.Validate(dto);
        if (!validationResult.IsValid)
            return BadRequest(validationResult.Errors.Select(x => new Error(x.ErrorCode, x.ErrorMessage)));

        MapUser? user = await _userManager.Users.Include(u => u.Testimonials).FirstOrDefaultAsync(u => u.Id == userId);
        if (user is null)
            return NotFound(new Error(EMapUserErrorCodes.UserNotFoundById.ToStringValue(), "Aucun utilisateur trouver avec cet Id"));

        Testimonial testimonial = _mapper.Map<AddTestimonialDto, Testimonial>(dto);

        await _testimonialPlatform.AddTestimonialAsync(user, testimonial);

        return CreatedAtAction(nameof(GetTestimonialById), new { testimonialId = testimonial.TestimonialId }, _mapper.Map<Testimonial, TestimonialDto>(testimonial));
    }

    /// <summary>
    /// Get Testimonial By Id
    /// </summary>
    /// <param name="testimonialId">Id of the wanted Testimonial</param>
    [HttpGet]
    [Route("{testimonialId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(TestimonialDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<TestimonialDto>> GetTestimonialById([FromRoute] int testimonialId)
    {

        Testimonial? testimonial = await _testimonialPlatform.GetByTestimonialIdAsync(testimonialId);
        if (testimonial is null)
            return NotFound(new Error(ETestimonialErrorCodes.TestimonialNotFoundById.ToStringValue(), "Aucun témoignage trouvé avec cet Id"));

        return _mapper.Map<Testimonial, TestimonialDto>(testimonial);
    }

    /// <summary>
    /// Get All Testimonials    
    /// </summary>
    [HttpGet]
    [Route("")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(ICollection<TestimonialDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public IQueryable<TestimonialDto> GetAllTestimonial()
    {
        IQueryable<Testimonial> testimonials = _testimonialPlatform.GetAllTestimonials();
        return _mapper.ProjectTo<TestimonialDto>(testimonials);
    }

    //[Authorize(Roles = Roles.Admin)]
    [HttpDelete]
    [Route("{testimonialId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult> DeleteTestimonialById([FromRoute] int testimonialId)
    {
        Testimonial? testimonial = await _testimonialPlatform.GetByTestimonialIdAsync(testimonialId);
        if (testimonial is null)
            return NotFound(new Error(ETestimonialErrorCodes.TestimonialNotFoundById.ToStringValue(), "Aucun témoignage trouvé avec cet Id"));


        await _testimonialPlatform.DeleteTestimonialAsync(testimonial);
        return NoContent();
    }
}