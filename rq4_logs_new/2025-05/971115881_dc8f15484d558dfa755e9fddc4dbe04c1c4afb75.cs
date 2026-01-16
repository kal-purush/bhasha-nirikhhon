using Microsoft.AspNetCore.Mvc;
using JadwalAPI.Model;
using JadwalAPI.Services;

namespace JadwalAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class Jadwal_Admin : ControllerBase
    {
        private readonly IJadwalService _jadwalService;

        public Jadwal_Admin(IJadwalService jadwalService)
        {
            _jadwalService = jadwalService;
        }

        [HttpGet]
        public ActionResult<List<JadwalModel>> GetAll()
        {
            return Ok(_jadwalService.GetAll());
        }

        [HttpGet("{tanggal}")]
        public ActionResult<JadwalModel> GetByTanggal(string tanggal)
        {
            if (!DateOnly.TryParse(tanggal, out DateOnly parsedDate))
                return BadRequest("Format tanggal tidak valid. Gunakan format yyyy-MM-dd.");

            var jadwal = _jadwalService.GetByTanggal(parsedDate);
            if (jadwal == null)
                return NotFound("Jadwal tidak ditemukan.");

            return Ok(jadwal);
        }

        [HttpPost]
        public ActionResult AddJadwal([FromBody] JadwalModel jadwal)
        {
            _jadwalService.TambahJadwal(jadwal);
            return CreatedAtAction(nameof(GetByTanggal), new { tanggal = jadwal.Tanggal.ToString("yyyy-MM-dd") }, jadwal);
        }

        [HttpPut("{tanggal}")]
        public ActionResult UpdateJadwal(string tanggal, [FromBody] JadwalModel updatedJadwal)
        {
            if (!DateOnly.TryParse(tanggal, out DateOnly parsedDate))
                return BadRequest("Format tanggal tidak valid. Gunakan format yyyy-MM-dd.");

            bool success = _jadwalService.UpdateJadwal(parsedDate, updatedJadwal);
            if (!success)
                return NotFound("Jadwal tidak ditemukan.");

            return NoContent();
        }

        [HttpDelete("{tanggal}")]
        public ActionResult DeleteJadwal(string tanggal)
        {
            if (!DateOnly.TryParse(tanggal, out DateOnly parsedDate))
                return BadRequest("Format tanggal tidak valid. Gunakan format yyyy-MM-dd.");

            bool success = _jadwalService.HapusJadwal(parsedDate);
            if (!success)
                return NotFound("Jadwal tidak ditemukan.");

            return NoContent();
        }
    }
}