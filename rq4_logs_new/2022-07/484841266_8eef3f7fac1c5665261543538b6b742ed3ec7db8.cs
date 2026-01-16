using System;
using System.Collections.Generic;
using System.Linq;
using GenerateMedicalDocuments.AppData.DirectionToMSE.Helpers.MainHelpers;
using GenerateMedicalDocuments.AppData.DirectionToMSE.Models;

namespace GenerateMedicalDocuments.AppData.DirectionToMSE.Helpers
{
    public class CreatingPrintFormHelper
    {
        #region Private fields

        /// <summary>
        /// Список параметров для шаблона документа.
        /// key - имя параметра;
        /// value - значение параметра;
        /// </summary>
        private Dictionary<string, string> parameters;

        private List<string> parametersName;

        /// <summary>
        /// Флаг, для установки "Галочки" в поле.
        /// </summary>
        private readonly string trueFlag = "V";
        
        #endregion

        public CreatingPrintFormHelper()
        {
            this.InitialParametersNames();
            this.InitialParametersList();
        }

        #region Private members

        #region Initialing

        /// <summary>
        /// Инициализация имен параметров для шаблона документа.
        /// </summary>
        private void InitialParametersNames()
        {
            this.parametersName = new List<string>()
            {
                "OrganizationName",
                "OrganizationAddress",
                "OrganizationOGRN",
                "DocumentNumber",
                "DocumentDate",
                "isToHome",
                "isNeedPaliatMedicalHelp",
                "isPrimaryProsthetics",
                
                "PatientFIO",
                "PatientBirthdate",
                "PatientAge",
                "isMan",
                "isWoman"
            };
        }
        
        /// <summary>
        /// Инициализация списка параметров для шаблона документа.
        /// </summary>
        private void InitialParametersList()
        {
            this.parameters = new Dictionary<string, string>();
            foreach (var name in this.parametersName)
            {
                this.parameters.Add(name, " ");
            }
        }

        #endregion


        public Dictionary<string, string> GetDataToParametersList(DirectionToMSEDocumentModel documentModel)
        {
            var patientAge = MainHelper.GetPatientAge(documentModel.RecordTarget.PatientRole.PatientData.BirthDate);
            
            this.SetOrganizationParameters(
                documentModel?.RepresentedCustodianOrganization?.Name,
                documentModel?.RepresentedCustodianOrganization?.Address?.StreetAddressLine,
                documentModel?.RepresentedCustodianOrganization?.Props?.OGRN);
            this.SetProtocolParameters(documentModel?.DocumentBody?.SentSection?.TargetSent?.Protocol?.ProtocolNumber,
                documentModel?.DocumentBody?.SentSection?.TargetSent?.Protocol?.ProtocolDate); // 1 point.
            this.SetIsToHomeParameter(documentModel?.DocumentBody?.SentSection?.SentParagraphs); // 2 point.
            this.SetIsNeedPaliatMedicalHelpParameter(documentModel?.DocumentBody?.SentSection?.SentParagraphs); // 3 point.
            this.SetIsPrimaryProsthetics(documentModel?.DocumentBody?.SentSection?.SentParagraphs); // 4 point.
            // 5.1 - 5.10 cells.
            // TODO: вероятно, нужно будет перенести в общий метод "SetPatienData".
            this.SetPatientData(documentModel?.RecordTarget?.PatientRole?.PatientData?.Name); // 6 point.
            
            this.parameters["PatientBirthdate"] =
                documentModel.RecordTarget.PatientRole.PatientData.BirthDate.ToString("dd MMMM yyyy");
            this.parameters["PatientAge"] = patientAge;
            if (documentModel.RecordTarget.PatientRole.PatientData.Gender.Code == "1")
            {
                this.parameters["isMan"] = "V";
            }
            if (documentModel.RecordTarget.PatientRole.PatientData.Gender.Code == "2")
            {
                this.parameters["isWoman"] = "V";
            }

            return this.parameters;
        }

        #region Set parameterst
        
        /// <summary>
        /// Устанавливает параметры организации.
        /// </summary>
        /// <param name="organizationName">Наименование организации.</param>
        /// <param name="organizationAddress">Адрес организации.</param>
        /// <param name="organizationOGRN">ОГРН организации.</param>
        private void SetOrganizationParameters(
            string organizationName, 
            string organizationAddress,
            string organizationOGRN)
        {
            if (!String.IsNullOrWhiteSpace(organizationName))
            {
                this.parameters["OrganizationName"] = organizationName;
            }

            if (!String.IsNullOrWhiteSpace(organizationAddress))
            {
                this.parameters["OrganizationAddress"] = organizationAddress;
            }

            if (!String.IsNullOrWhiteSpace(organizationOGRN))
            {
                this.parameters["OrganizationOGRN"] = organizationOGRN;
            }
        }

        /// <summary>
        /// Устанавливает параметры протокола врачебной комиссии. (1 пункт).
        /// </summary>
        /// <param name="protocolNumber">Номер протокола.</param>
        /// <param name="protocolDate">Дата протокола.</param>
        private void SetProtocolParameters(string protocolNumber, DateTime? protocolDate)
        {
            if (!String.IsNullOrWhiteSpace(protocolNumber))
            {
                this.parameters["DocumentNumber"] = protocolNumber;
            }

            if (protocolDate is not null)
            {
                this.parameters["DocumentDate"] = protocolDate?.ToString("dd MMMM yyyy");
            }
        }

        /// <summary>
        /// Устанавливает флаг в ячейку "медико-социальную экспертизу необходимо проводить на дому". (2 пункт).
        /// </summary>
        /// <param name="paragraphs">Параграфы секции "Направление".</param>
        private void SetIsToHomeParameter(List<ParagraphModel> paragraphs)
        {
            var isToHome = this.GetFlagParameterInParagraphs(paragraphs,
                ValidationCaptions.PersonNotComeToOffice, ValidationContents.MedicalExaminationNeedAtHome);
            if (isToHome)
            {
                this.parameters["isToHome"] = trueFlag;
            }
        }

        /// <summary>
        /// Установка флага в ячейку "Гражданин нуждается в оказании паллиативной медицинской помощи". (3 пункт).
        /// </summary>
        /// <param name="paragraphs">Параграфы секции "Направление".</param>
        private void SetIsNeedPaliatMedicalHelpParameter(List<ParagraphModel> paragraphs)
        {
            var isNeedPaliatMedicalHelp = this.GetFlagParameterInParagraphs(paragraphs,
                ValidationCaptions.PersonNeedsPaliativeHelp, ValidationContents.NeedPalliativeMedicalHelp);
            if (isNeedPaliatMedicalHelp)
            {
                this.parameters["isNeedPaliatMedicalHelp"] = trueFlag;
            }
        }

        /// <summary>
        /// Установка флага в ячейку "Гражданин нуждающийся в первичном протезировании". (4 пункт).
        /// </summary>
        /// <param name="paragraphs">Параграфы секции "Направление".</param>
        private void SetIsPrimaryProsthetics(List<ParagraphModel> paragraphs)
        {
            var isNeedPaliatMedicalHelp = this.GetFlagParameterInParagraphs(paragraphs,
                ValidationCaptions.PersonNeedsPrimaryProsthetics, ValidationContents.NeedPrimaryProsthetics);
            if (isNeedPaliatMedicalHelp)
            {
                this.parameters["isPrimaryProsthetics"] = trueFlag;
            }
        }

        /// <summary>
        /// Устаналивает ФИО пациента. (6 пункт);
        /// </summary>
        /// <param name="nameModel">Модель имени.</param>
        private void SetPatientData(NameModel nameModel)
        {
            this.parameters["PatientFIO"] = this.GetFIO(nameModel);
        }
        
        #endregion

        #region Helpers methods
        
        /// <summary>
        /// Получить флаг по наименованию и значению параграфа.
        /// </summary>
        /// <param name="paragraphs">Параграфы секции.</param>
        /// <param name="caption">Наименование.</param>
        /// <param name="content">Значение.</param>
        /// <returns>Истина - устанавливаем флаг.</returns>
        private bool GetFlagParameterInParagraphs(
            List<ParagraphModel> paragraphs, 
            string caption, 
            string content)
        {
            var paragraph = paragraphs.FirstOrDefault(p =>
                p.Caption == caption);
            
            return paragraph is null ? false : paragraph.Content.Contains(content);
        }

        /// <summary>
        /// Получить строку "Фамилия Имя Отчество".
        /// </summary>
        /// <param name="nameModel">Модель имени.</param>
        /// <returns>Строка "Фамилия Имя Отчество".</returns>
        private string GetFIO(NameModel nameModel)
        {
            if (nameModel is null)
            {
                return " ";
            }
            
            string patientFIO = " ";
            if (!String.IsNullOrWhiteSpace(nameModel.Family))
            {
                patientFIO += $"{nameModel.Family} ";
            }

            if (!String.IsNullOrWhiteSpace(nameModel.Given))
            {
                patientFIO += $"{nameModel.Given} ";
            }
            
            if (!String.IsNullOrWhiteSpace(nameModel.Patronymic))
            {
                patientFIO += nameModel.Patronymic;
            }

            return patientFIO;
        }
        
        #endregion
        
        #endregion
    }
}