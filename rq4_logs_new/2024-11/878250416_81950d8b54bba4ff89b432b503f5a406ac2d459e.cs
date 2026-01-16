using System.Xml.Linq;
using MedicalExaminationPreliminaryLists.Data.Models;
using MedicalExaminationPreliminaryLists.Share.DTOs;

namespace MedicalExaminationPreliminaryLists.Api.Application.Services
{
    public class MedicalExaminationPreliminaryListReader
    {
        static MedicalExaminationPreliminaryListParse parser = new();
        public static List<ZAP> ReadFromXML (string filePath)
        {
            List<ZAP> zapList = [];

            XDocument xdoc = XDocument.Load(filePath);
            XElement? dn = xdoc.Element("DN");
            if (dn is not null)
            {
                foreach (XElement zap in dn.Elements("ZAP"))
                {
                    ZAPModel zapDTO = parser.GetZAP(zap);

                    ZAP zapNew = new()
                    {
                        ZAPNumber = zapDTO.ZAPNumber,
                        Birthday = zapDTO.Birthday,
                        Surname = zapDTO.Surname,
                        Name1 = zapDTO.Surname,
                        Name2 = zapDTO.Surname
                    };

                    zapList.Add(zapNew);
                }
            }
            return zapList;
        }
    }
}