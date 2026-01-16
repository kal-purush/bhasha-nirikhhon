using Microsoft.AspNetCore.Mvc;
using JadwalAPI.Model;
// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace JadwalAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class JadwalController : ControllerBase
    {
        private static List<JadwalModel_Admin> jadwalList = new List<JadwalModel_Admin>
        {
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now), JenisSampah = new List<string> { "Organik" }, namaKurir = "Andi" },
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now.AddDays(1)), JenisSampah = new List<string> { "Plastik" }, namaKurir = "Budi" },
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now.AddDays(2)), JenisSampah = new List<string> { "Kertas" }, namaKurir = "Joko" },
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now.AddDays(3)), JenisSampah = new List<string> { "Logam" }, namaKurir = "Oka" },
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now.AddDays(4)), JenisSampah = new List<string> { "Elektronik" }, namaKurir = "Eka" },
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now.AddDays(5)), JenisSampah = new List<string> { "BahanBerbahaya" }, namaKurir = "Herawan" },
            new JadwalModel_Admin { Tanggal = DateOnly.FromDateTime(DateTime.Now.AddDays(6)), JenisSampah = new List<string> { "Minyak" }, namaKurir = "Tono" }
        };

        [HttpGet]
        public ActionResult<List<JadwalModel_Admin>> GetAll()
        {
            return Ok(jadwalList);
        }

        [HttpGet("{tanggal}")]
        public ActionResult<JadwalModel_Admin> GetByTanggal(string tanggal)
        {
            if (!DateOnly.TryParse(tanggal, out DateOnly parsedDate))
                return BadRequest("Format tanggal tidak valid. Gunakan format yyyy-MM-dd.");

            var jadwal = jadwalList.FirstOrDefault(j => j.Tanggal == parsedDate);
            if (jadwal == null)
                return NotFound("Jadwal tidak ditemukan.");

            return Ok(jadwal);
        }

        [HttpPost]
        public ActionResult AddJadwal([FromBody] JadwalModel_Admin jadwal)
        {
            jadwalList.Add(jadwal);
            return CreatedAtAction(nameof(GetByTanggal), new { tanggal = jadwal.Tanggal.ToString("yyyy-MM-dd") }, jadwal);
        }

        [HttpPut("{tanggal}")]
        public ActionResult UpdateJadwal(string tanggal, [FromBody] JadwalModel_Admin updatedJadwal)
        {
            if (!DateOnly.TryParse(tanggal, out DateOnly parsedDate))
                return BadRequest("Format tanggal tidak valid. Gunakan format yyyy-MM-dd.");

            var jadwal = jadwalList.FirstOrDefault(j => j.Tanggal == parsedDate);
            if (jadwal == null)
                return NotFound("Jadwal tidak ditemukan.");

            jadwal.JenisSampah = updatedJadwal.JenisSampah;
            jadwal.namaKurir = updatedJadwal.namaKurir;
            return NoContent();
        }

        [HttpDelete("{tanggal}")]
        public ActionResult DeleteJadwal(string tanggal)
        {
            if (!DateOnly.TryParse(tanggal, out DateOnly parsedDate))
                return BadRequest("Format tanggal tidak valid. Gunakan format yyyy-MM-dd.");

            var jadwal = jadwalList.FirstOrDefault(j => j.Tanggal == parsedDate);
            if (jadwal == null)
                return NotFound("Jadwal tidak ditemukan.");

            jadwalList.Remove(jadwal);
            return NoContent();
        }
    }
}