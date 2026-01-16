using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Neo4jClient;
using Neo4jClient.Cypher;

namespace Neo4jDataAnalyzer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AnalyzerController : ControllerBase
    {
        private readonly IGraphClient _client;

        public AnalyzerController(IGraphClient client)
        {
            _client = client;
        }

        [HttpGet("DoctorWithMostPrescriptions")]
        public async Task<ActionResult> DoctorWithMostPrescriptions()
        {
            var items = await _client.Cypher.Match("(d:Doctor)-[r: prescribed_by]-(p: Prescription)")
                .Return(() => new
                {
                    Doctor = Return.As<string>("d.LastName"),
                    AmountOfPrescription = Return.As<int>("count(r)")
                }).OrderByDescending("count(r)").Limit(10).ResultsAsync;
            return Ok(items);
        }
        [HttpGet("PharmacyWithMostEmployees")]
        public async Task<ActionResult> PharmacyWithMostEmployees()
        {
            var items = await _client.Cypher.Match("(p: Pharmacy) -[r: work_for] - (ph: Pharamceut)")
                .Return(() => new
                {
                    Pharmacy = Return.As<string>("p.Name"),
                    AmountOfEmployees = Return.As<int>("count(r)")
                }).OrderByDescending("count(r)").Limit(10).ResultsAsync;
            return Ok(items);
        }
        [HttpGet("DoctorsComparedWithMedicines")]
        public async Task<ActionResult> DoctorsComparedWithMedicines()
        {
            var items = await _client.Cypher.Match("(m: Medicine),(p: Prescription),(d: Doctor)")
                .Where("d.Id = p.DoctorId and p.MedicineName = m.Name")
                .Return(() => new
                {
                    Medicine = Return.As<string>("m.Name"),
                    Doctor = Return.As<string>("d.LastName"),
                    AmountOfPrescriptions = Return.As<int>("count(p)")
                }).OrderByDescending("count(p)").ResultsAsync;
            return Ok(items);
            

        }
    }
}