using DistributedPatientHealthCareSystem.DPHCSModels;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DistributedPatientHealthCareSystem.Controllers
{
    public class tempController:Controller
    {
        private DPHCSContext context = null;
        public tempController(DPHCSContext _context)
        {
            context = _context;
        }
        public String test()
        {

            PatientHealthRecord phr = context.PatientHealthRecord.FirstOrDefault();
            //PatientPrescription p = new PatientPrescription();
            //p.MedicineName = "MedicineName1";
            //p.Quantity = "Quantity1";
            //p.UsageDirections = "UsageDirections1";
            //phr.PatientPrescription.Add(p);
            //PatientPrescription p2 = new PatientPrescription();
            //p2.MedicineName = "MedicineName2";
            //p2.Quantity = "Quantity2";
            //p2.UsageDirections = "UsageDirections2";
            //phr.PatientPrescription.Add(p);
            //phr.PatientPrescription.Add(p2);
            String phrJson = JsonConvert.SerializeObject(phr);
            return phrJson;
        }

    }
}