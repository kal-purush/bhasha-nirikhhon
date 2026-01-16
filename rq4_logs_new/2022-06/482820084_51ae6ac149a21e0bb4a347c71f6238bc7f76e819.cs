using AutoMapper;
using DataAnalyzer.Model;
using DataAnalyzingService.Model;
using lib.DTO;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Neo4jClient;
using Newtonsoft.Json;

namespace DataAnalyzer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SupplierController : ControllerBase
    {
        private readonly IMapper _mapper;
        private readonly IGraphClient _clientGraph;
        private readonly HttpClient _httpConsultationClient;
        private readonly HttpClient _httpPerscriptionClient;

        public SupplierController(IGraphClient clientGraph, IMapper mapper, IHttpClientFactory factory)
        {
            _clientGraph = clientGraph;
            _mapper = mapper;
            _httpConsultationClient = factory.CreateClient("ConsultationClient");
            _httpPerscriptionClient = factory.CreateClient("PrescriptionClient");
        }

        [HttpGet("RunAllSuppliers")]
        public async Task<string> Run()
        {
            await Patients();
            await Pharmacies();
            await Prescriptions();
            await Pharamceuts();
            await Medicines();
            await Doctors();
            await Consultations();
            await Prescribed();
            await Prescribed_By();
            await Work_For();
            await Booked();
            await Schedule_For();
            await Prescribed_To();

            return "Task Completed";
        }

        [HttpGet("DoctorSupplier")]
        public async Task<IEnumerable<PersonNDto>> Doctors()
        {
            string content = await _httpPerscriptionClient.GetStringAsync("Persons/doctors?Size=1000");
            IList<PersonDto> protodoctors = JsonConvert.DeserializeObject<IList<PersonDto>>(content);
            var doctors = _mapper.Map<IList<PersonDto>, IList<PersonNDto>>(protodoctors);
            foreach (PersonNDto item in doctors)
            {
                await _clientGraph.Cypher.Merge("(d:Doctor {Id: $dID } )")
                    .OnMatch()
                    .Set("d=$doc")
                    .OnCreate()
                    .Set("d=$doc")
                    .WithParam("doc", item)
                    .WithParam("dID", item.Id)
                    .ExecuteWithoutResultsAsync();
            }
            return doctors;
        }
        [HttpGet("MedicineSupplier")]

        public async Task<IEnumerable<string>> Medicines()
        {
            string content = await _httpPerscriptionClient.GetStringAsync("Medicines");
            IList<string> medicines = JsonConvert.DeserializeObject<IList<string>>(content);
            foreach (string item in medicines)
            {
                await _clientGraph.Cypher.Merge("(m:Medicine {Name: $med } )")
                    .WithParam("med", item)
                    .ExecuteWithoutResultsAsync();
            }



            return medicines;
        }
        [HttpGet("PharamceutSupplier")]

        public async Task<IEnumerable<PersonNDto>> Pharamceuts()
        {
            string content = await _httpPerscriptionClient.GetStringAsync("Persons/pharmaceuts?Size=999");
            IList<PersonDto> protopharamceuts = JsonConvert.DeserializeObject<IList<PersonDto>>(content);
            var pharamceuts = _mapper.Map<IList<PersonDto>, IList<PersonNDto>>(protopharamceuts);
            foreach (PersonNDto item in pharamceuts)
            {
                await _clientGraph.Cypher.Merge("(p:Pharamceut {Id: $pID } )")
                    .OnMatch()
                    .Set("p=$phar")
                    .OnCreate()
                    .Set("p=$phar")
                    .WithParam("phar", item)
                    .WithParam("pID", item.Id)
                    .ExecuteWithoutResultsAsync();
            }



            return pharamceuts;
        }
        [HttpGet("PatientSupplier")]
        public async Task<IEnumerable<PersonNDto>> Patients()
        {
            string content = await _httpPerscriptionClient.GetStringAsync("Persons/patients?Size=10001");
            IList<PersonDto> protopatients = JsonConvert.DeserializeObject<IList<PersonDto>>(content);
            var patients = _mapper.Map<IList<PersonDto>, IList<PersonNDto>>(protopatients);
            foreach (PersonNDto item in patients)
            {
                await _clientGraph.Cypher.Merge("(p:Patient {Id: $pID } )")
                    .OnMatch()
                    .Set("p=$pat")
                    .OnCreate()
                    .Set("p=$pat")
                    .WithParam("pat", item)
                    .WithParam("pID", item.Id)
                    .ExecuteWithoutResultsAsync();
            }



            return patients;
        }
        [HttpGet("PrescriptionSupplier")]
        public async Task<IEnumerable<PrescriptionNDto>> Prescriptions()
        {

            string content = await _httpPerscriptionClient.GetStringAsync("Prescriptions?Size=20001");
            IList<PrescriptionDto> protoPrescription = JsonConvert.DeserializeObject<IList<PrescriptionDto>>(content);
            var prescriptions = _mapper.Map<IList<PrescriptionDto>, IList<PrescriptionNDto>>(protoPrescription);
            foreach (PrescriptionNDto item in prescriptions)
            {
                await _clientGraph.Cypher.Merge("(p:Prescription {Id: $pID} )")
                    .OnMatch()
                    .Set("p=$pre")
                    .OnCreate()
                    .Set("p=$pre")
                    .WithParam("pre", item)
                    .WithParam("pID", item.Id)
                    .ExecuteWithoutResultsAsync();
            }



            return prescriptions;
        }
        [HttpGet("PharmacySupplier")]
        public async Task<IEnumerable<PharmacyNDto>> Pharmacies()
        {
            string content = await _httpPerscriptionClient.GetStringAsync("Pharmacies?Size=100");
            IList<PharmacyDto> protopharmacies = JsonConvert.DeserializeObject<IList<PharmacyDto>>(content);
            var pharmacies = _mapper.Map<IList<PharmacyDto>, IList<PharmacyNDto>>(protopharmacies);
            foreach (PharmacyNDto item in pharmacies)
            {
                await _clientGraph.Cypher.Merge("(p:Pharmacy {Id: $pID} )")
                    .OnMatch()
                    .Set("p=$pha")
                    .OnCreate()
                    .Set("p=$pha")
                    .WithParam("pha", item)
                    .WithParam("pID", item.Id)
                    .ExecuteWithoutResultsAsync();
            }


            return pharmacies;
        }
        [HttpGet("ConsultationSupplier")]
        public async Task<IEnumerable<ConsultationNDto>> Consultations()
        {
            string content = await _httpConsultationClient.GetStringAsync("Consultation");
            IList<ConsultationDto> protoConsultations = JsonConvert.DeserializeObject<IList<ConsultationDto>>(content);
            var consultations = _mapper.Map<IList<ConsultationDto>, IList<ConsultationNDto>>(protoConsultations);

            foreach (ConsultationNDto item in consultations)
            {
                await _clientGraph.Cypher.Merge("(c:Consultation {Id: $cID} )")
                    .OnMatch()
                    .Set("c=$con")
                    .OnCreate()
                    .Set("c=$con")
                    .WithParam("con", item)
                    .WithParam("cID", item.Id)
                    .ExecuteWithoutResultsAsync();
            }

            return consultations;
        }
        [HttpGet("AddPrescribedRelation")]
        public async Task<string> Prescribed()
        {
            await _clientGraph.Cypher.Match("(p:Prescription)", "(m:Medicine)")
                .Where((PrescriptionDto p, MedicineDto m) => p.MedicineName == m.Name)
                .Merge("(p)-[:prescribed]->(m)")
                .ExecuteWithoutResultsAsync();

            return "Task Completed for method Prescribed";
        }
        [HttpGet("AddPrescribed_ByRelation")]
        public async Task<string> Prescribed_By()
        {
            await _clientGraph.Cypher.Match("(d:Doctor)", "(p:Prescription)")
                .Where((PersonNDto d, PrescriptionNDto p) => d.Id == p.DoctorId)
                .Merge("(d)-[:prescribed_by]->(p)")
                .ExecuteWithoutResultsAsync();

            return "Task Completed for method Prescribed_By";
        }
        [HttpGet("AddPrescribed_ToRelation")]
        public async Task<string> Prescribed_To()
        {
            await _clientGraph.Cypher.Match("(p:Prescription)", "(pa:Patient)")
                .Where((PersonNDto pa, PrescriptionNDto p) => pa.Id == p.PatientId)
                .Merge("(p)-[:prescribed_to]->(pa)")
                .ExecuteWithoutResultsAsync();

            return "Task Completed for method Prescribed_To";
        }
        [HttpGet("AddSchedule_ForRelation")]
        public async Task<string> Schedule_For()
        {
            await _clientGraph.Cypher.Match("(c:Consultation)", "(d:Doctor)")
                .Where("c.DoctorId = d.Id")
                .Merge("(c)-[:schedule_For]->(d)")
                .ExecuteWithoutResultsAsync();

            return "Task Completed for method Schedule_For";
        }
        [HttpGet("AddBookedRelation")]
        public async Task<string> Booked()
        {
            await _clientGraph.Cypher.Match("(c:Consultation)", "(p:Patient)")
                .Where("c.PatientId = p.Id")
                .Merge("(p)-[:booked]->(c)")
                .ExecuteWithoutResultsAsync();

            return "Task Completed for method Booked";
        }
        [HttpGet("AddWork_ForRelation")]
        public async Task<string> Work_For()
        {
            await _clientGraph.Cypher.Match("(pha:Pharmacy)", "(p:Pharamceut)")
                .Where("p.PharmacyName = pha.Name")
                .Merge("(p)-[:work_for]->(pha)")
                .ExecuteWithoutResultsAsync();

            return "Task Completed for method Work_For";
        }

    }
}