using Microsoft.AspNetCore.Mvc;
using OnlineLearningWebAPI.DTOs.request.CertificateRequest;
using OnlineLearningWebAPI.Service.IService;

namespace OnlineLearningWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CertificateController : ControllerBase
    {
        private readonly ICertificateService _certificateService;

        public CertificateController(ICertificateService certificateService)
        {
            _certificateService = certificateService;
        }

        [HttpGet]
        public async Task<IActionResult> GetAllCertificates()
        {
            var certificates = await _certificateService.GetAllCertificatesAsync();
            return Ok(certificates);
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetCertificateById(int id)
        {
            var certificate = await _certificateService.GetCertificateByIdAsync(id);
            if (certificate == null) return NotFound(new { message = "Certificate not found" });

            return Ok(certificate);
        }

        [HttpPost]
        public async Task<IActionResult> CreateCertificate([FromBody] CreateCertificateDTO createCertificateDTO)
        {
            var result = await _certificateService.CreateCertificateAsync(createCertificateDTO);
            if (!result) return BadRequest(new { message = "Failed to create certificate" });

            return Ok(new { message = "Certificate created successfully" });
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateCertificate(int id, [FromBody] UpdateCertificateDTO updateCertificateDTO)
        {
            var result = await _certificateService.UpdateCertificateAsync(id, updateCertificateDTO);
            if (!result) return NotFound(new { message = "Certificate not found or update failed" });

            return Ok(new { message = "Certificate updated successfully" });
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteCertificate(int id)
        {
            var result = await _certificateService.DeleteCertificateAsync(id);
            if (!result) return NotFound(new { message = "Certificate not found or delete failed" });

            return Ok(new { message = "Certificate deleted successfully" });
        }
    }
}