using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using GenerateMedicalDocuments.AppData.DirectionToMSE.Models;

namespace GenerateMedicalDocuments.AppData.DirectionToMSE.Helpers
{
    public class CreatingHTMLDocumentHelper
    {
        #region Private fields

        private StreamWriter StreamWriter;

        #endregion

        #region Constructor

        public CreatingHTMLDocumentHelper(string saveFilePatch)
        {
            this.StreamWriter = new StreamWriter(saveFilePatch);
        }

        #endregion
        
        /// <summary>
        /// Генерирует и сохраняет HTML документ "Направление на медико-социальную экспертизу" по модели документа.
        /// </summary>
        /// <param name="documentModel">Модель документа.</param>
        public void CreateHTMLDocument(DirectionToMSEDocumentModel documentModel)
        {
            if (documentModel == null)
            {
                return;
            }
            
            this.GenerateHtmlTag(documentModel);
        }

        /// <summary>
        /// Создает тег "html" с содержимым.
        /// </summary>
        private void GenerateHtmlTag(DirectionToMSEDocumentModel documentModel)
        {
            if (documentModel is null)
            {
                return;
            }
            
            this.OpenHtmlTag();
            this.GenerateHeadTag(documentModel.CreateDate);
            this.GenerateBodyTag(documentModel);
            this.CloseHtmlTag();
        }

        /// <summary>
        /// Создает тег "head" с содержимым.
        /// </summary>
        private void GenerateHeadTag(DateTime createDate)
        {
            this.StreamWriter.WriteLine("   <head>");
            this.StreamWriter.WriteLine($"      <title>Направление на медико-социальную экспертизу от {createDate.ToString("dd MMMM yyyy")}</title>");
            this.GenerateStyleTag();
            this.StreamWriter.WriteLine("   </head>");
        }

        /// <summary>
        /// Создает тег "style" с содержимым.
        /// </summary>
        private void GenerateStyleTag()
        {
            this.StreamWriter.WriteLine("       <style type=\"text/css\" media=\"screen\">");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               table.outer");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               width:100%;");
            this.StreamWriter.WriteLine("               cellspacing:0;");
            this.StreamWriter.WriteLine("               cellpadding:0;");
            this.StreamWriter.WriteLine("               border: solid 2px #999999;");
            this.StreamWriter.WriteLine("               border-collapse: collapse;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               td.outer , th.outer");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               font: 14px Arial, Helvetica, sans-serif;");
            this.StreamWriter.WriteLine("               color: #003366;");
            this.StreamWriter.WriteLine("               border: solid 2px #999999;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               th.outer");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               background: #EEEEEE;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               td.outter");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               background: #fff;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               table.Sections");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               border: none;");
            this.StreamWriter.WriteLine("               border-collapse: collapse;");
            this.StreamWriter.WriteLine("               width: 100% ;");
            this.StreamWriter.WriteLine("               margins: 0,0,0,0;");
            this.StreamWriter.WriteLine("               padding: 0,0,0,0;");
            this.StreamWriter.WriteLine("               cellspacing:0;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               h2");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               font: bold 24px Arial, Helvetica, sans-serif;");
            this.StreamWriter.WriteLine("               color: #333333;");
            this.StreamWriter.WriteLine("               text-align: left;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               td.SectionTitle");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               border-top: solid 4px #999999;");
            this.StreamWriter.WriteLine("               font-style: italic;");
            this.StreamWriter.WriteLine("               font-size: 12px;");
            this.StreamWriter.WriteLine("               background: #EEEEEE;");
            this.StreamWriter.WriteLine("               text-align:left;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               td.SubSectionTitle");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               border:none;");
            this.StreamWriter.WriteLine("               width: 6% ;");
            this.StreamWriter.WriteLine("               font: bold 12px Arial, Helvetica, sans-serif;");
            this.StreamWriter.WriteLine("               text-align: right;");
            this.StreamWriter.WriteLine("               vertical-align : top ;");
            this.StreamWriter.WriteLine("               padding-right: 5px;");
            this.StreamWriter.WriteLine("               padding-top: 4px;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               td.Rest");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               border-top: none;");
            this.StreamWriter.WriteLine("               border-right: none;");
            this.StreamWriter.WriteLine("               border-bottom: 1px solid #CCCCCC;");
            this.StreamWriter.WriteLine("               border-left: none;");
            this.StreamWriter.WriteLine("               font: 14px Arial, Helvetica, sans-serif;");
            this.StreamWriter.WriteLine("               color: #003366;");
            this.StreamWriter.WriteLine("               text-align: bottom;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               tr");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               vertical-align:top;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               table.inner");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               cellspacing:1;");
            this.StreamWriter.WriteLine("               cellpadding:5;");
            this.StreamWriter.WriteLine("               border: solid 2px #999999;");
            this.StreamWriter.WriteLine("               border-collapse: collapse;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               table.inner th, table.inner td, table.inner tr");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               font: 14px Arial, Helvetica, sans-serif;");
            this.StreamWriter.WriteLine("               color: #003366;");
            this.StreamWriter.WriteLine("               border: solid 2px #999999;");
            this.StreamWriter.WriteLine("               padding: 2px 5px;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               th.inner");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               background: #EEEEEE;");
            this.StreamWriter.WriteLine("               }");
            
            this.StreamWriter.WriteLine("");
            this.StreamWriter.WriteLine("               td.inner");
            this.StreamWriter.WriteLine("               {");
            this.StreamWriter.WriteLine("               background: #fff;");
            this.StreamWriter.WriteLine("               }");
            this.StreamWriter.WriteLine("");
            
            this.StreamWriter.WriteLine("       </style>");
        }
        
        /// <summary>
        /// Создает тег "body" с содержимым.
        /// </summary>
        private void GenerateBodyTag(DirectionToMSEDocumentModel documentModel)
        {
            if (documentModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("   <body>"); // 1 tab.
            this.GenerateMedicalOrginazationTable(documentModel.RecordTarget.PatientRole.ProviderOrganization);
            this.StreamWriter.WriteLine($"      <h2>Направление на медико-социальную экспертизу от {documentModel.CreateDate.ToString("dd MMMM yyyy")}</h2>"); // 2 tabs.
            this.GeneratePatientInfoTable(documentModel.RecordTarget.PatientRole, documentModel.Participant.ScopingOrganization.Name);
            this.GenerateSectionsTable(documentModel);
            this.StreamWriter.WriteLine("   </body>");
        }

        #region Medical organization table

        /// <summary>
        /// Создает таблицу "Медецинская организация".
        /// </summary>
        /// <param name="organizationModel">Модель организации.</param>
        private void GenerateMedicalOrginazationTable(OrganizationModel organizationModel)
        {
            if (organizationModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("       <table class=\"outer\" width=\"100%\">"); // 2 tabs.
            this.StreamWriter.WriteLine("           <col width=\"40%\"/>"); // 3 tabs.
            this.StreamWriter.WriteLine("           <col width=\"60%\"/>");
            this.StreamWriter.WriteLine("           <tr class=\"outer\">");
            this.GenerateMedicalOrginazationTableHeader();
            this.GenerateMedicalOrginazationTableData(organizationModel);
            this.StreamWriter.WriteLine("           </tr>");
            this.StreamWriter.WriteLine("       </table>"); // 2 tabs.
        }

        /// <summary>
        /// Создает заголовок таблицы "Медицинская организация".
        /// </summary>
        private void GenerateMedicalOrginazationTableHeader()
        {
            this.StreamWriter.WriteLine("               <th class=\"outer\" width=\"20%\">"); // 4 tabs.
            this.StreamWriter.WriteLine("                   Медицинская организация:"); // 5 tabs.
            this.StreamWriter.WriteLine("               </th>");
        }

        /// <summary>
        /// Создает блок данных таблицы "Медицинская организация".
        /// </summary>
        /// <param name="organizationModel">Модель организации.</param>
        private void GenerateMedicalOrginazationTableData(OrganizationModel organizationModel)
        {
            if (organizationModel is null)
            {
                return;
            }
            
            string organizationAddress = String.Empty;
            string organizationLicense = String.Empty;

            if (organizationModel.Address is not null)
            {
                if (organizationModel.Address.PostalCode is not null)
                {
                    organizationAddress += organizationModel.Address.PostalCode + ", ";
                }

                if (organizationModel.Address.StreetAddressLine is not null)
                {
                    organizationAddress += organizationModel.Address.StreetAddressLine;
                }
            }

            if (organizationModel.License is not null)
            {
                if (organizationModel.License.Number is not null)
                {
                    organizationLicense += organizationModel.License.Number + ", ";
                }

                if (organizationModel.License.AssigningAuthorityName is not null)
                {
                    organizationLicense += organizationModel.License.AssigningAuthorityName;
                }
            }

            string contacts = this.GetContactsString(organizationModel.Contacts);
            
            this.StreamWriter.WriteLine("               <td class=\"outer\">"); // 4 tabs.
            this.StreamWriter.WriteLine($"                   <strong>Название медицинской организации: </strong> {organizationModel.Name} <br/>"); // 5 tabs.
            
            if (!String.IsNullOrEmpty(organizationAddress))
            {
                this.StreamWriter.WriteLine($"                   <strong>Адрес: </strong> {organizationAddress} <br/>");
            }

            if (!String.IsNullOrEmpty(organizationLicense))
            {
                this.StreamWriter.WriteLine($"                   <strong>Лицензия: </strong> {organizationLicense} <br/>");
            }

            if (!String.IsNullOrEmpty(contacts))
            {
                this.StreamWriter.WriteLine($"                   <strong>Контакты: </strong> {contacts} <br/>");
            }
            
            this.StreamWriter.WriteLine("               </td>");
        }

        #endregion

        #region Patient info table

        /// <summary>
        /// Создает таблицу "Информация о пациенте" с содержимым.
        /// </summary>
        /// <param name="patientModel">Модель пациента.</param>
        /// <param name="scopingOrganizationName">Наименование страховой организации.</param>
        private void GeneratePatientInfoTable(PatientModel patientModel, string scopingOrganizationName)
        {
            if (patientModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("       <table class=\"outer\" width=\"100%\">"); // 2 tabs.
            this.StreamWriter.WriteLine("           <col width=\"40%\"/>"); // 3 tabs.
            this.StreamWriter.WriteLine("           <col width=\"60%\"/>");
            
            this.GeneratePatientInfoTableRows(patientModel, scopingOrganizationName);
            
            this.StreamWriter.WriteLine("       </table>");
            this.StreamWriter.WriteLine("       <br/>");
        }

        /// <summary>
        /// Создает блоки данных таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="patientModel"></param>
        /// <param name="scopingOrganizationName">Наименование страховой организации.</param>
        private void GeneratePatientInfoTableRows(PatientModel patientModel, string scopingOrganizationName)
        {
            this.GeneratePatientInfoTableHead("Пациент:");
            this.GeneratePatientInfoTableDateName(patientModel.PatientData.Name);
            
            this.GeneratePatientInfoTableHead("Пол пациента:");
            this.GeneratePatientInfoTableDataGender(patientModel.PatientData.Gender.DisplayName);
            
            this.GeneratePatientInfoTableHead("Дата рождения (Возраст):");
            this.GeneratePatientInfoTableDataBirtday(patientModel.PatientData.BirthDate);
            
            this.GeneratePatientInfoTableHead("Идентификаторы пациента:");
            this.GeneratePatientInfoTableDataIdentity(patientModel.SNILS, patientModel.InsurancePolicy, scopingOrganizationName);
            
            this.GeneratePatientInfoTableHead("Документ, удостоверяющий личность:");
            this.GeneratePatientInfoTableDataIdentityDocument(patientModel.IdentityDocument);
            
            this.GeneratePatientInfoTableHead("Контактная информация:");
            var contacts = patientModel.Contacts;
            if (patientModel.ContactPhoneNumber is not null)
            {
                contacts.Add(patientModel.ContactPhoneNumber);
            }
            this.GeneratePatientInfoTableDataContactInfo(patientModel.PermanentAddress, patientModel.ActualAddress, contacts);
        }

        /// <summary>
        /// Создает заголовок таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="caption">Текст в заголовке.</param>
        private void GeneratePatientInfoTableHead(string caption)
        {
            this.OpenPatientInfoTableRowTag();
            
            this.StreamWriter.Write("               <th class=\"outer\" width=\"20%\">"); // 4 tabs.
            this.StreamWriter.WriteLine($"{caption} </th>");
        }

        #region Generate table data

        /// <summary>
        /// Создает разметку данных имени пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="nameModel">Имя пациента.</param>
        private void GeneratePatientInfoTableDateName(NameModel nameModel)
        {
            if (nameModel is null)
            {
                return;
            }
            
            string nameString = String.Empty;

            if (nameModel.Family is not null)
            {
                nameString += nameModel.Family + " ";
            }

            if (nameModel.Given is not null)
            {
                nameString += nameModel.Given + " ";
            }
            
            if (nameModel.Patronymic is not null)
            {
                nameString += nameModel.Patronymic;
            }
            
            this.StreamWriter.WriteLine("               <td class=\"outer\">"); // 4 tabs.
            this.StreamWriter.WriteLine($"                   <b>{nameString}</b>"); // 5 tabs.
            this.StreamWriter.WriteLine("               </td>");
            
            this.ClosePatientInfoTableRowTag();
        }

        /// <summary>
        /// Создает разметку данных пола пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="gender">Пол пациента.</param>
        private void GeneratePatientInfoTableDataGender(string gender)
        {
            if (!String.IsNullOrWhiteSpace(gender))
            {
                this.StreamWriter.Write("               <td class=\"outer\">"); // 4 tabs.
                this.StreamWriter.Write(gender); // 5 tabs.
                this.StreamWriter.WriteLine("</td>");
            }

            this.ClosePatientInfoTableRowTag();
        }

        /// <summary>
        /// Создает разметку данных даты рождения пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="birthday">Дата рождения.</param>
        private void GeneratePatientInfoTableDataBirtday(DateTime birthday)
        {
            var age = this.GetPatientAge(birthday);
            
            this.StreamWriter.Write("               <td class=\"outer\">"); // 4 tabs.
            this.StreamWriter.Write($"{birthday.ToString("dd.MM.yyyy")} ({age})");
            this.StreamWriter.WriteLine("</td>");
            
            this.ClosePatientInfoTableRowTag();
        }

        /// <summary>
        /// Создает разметку данных идентификаторов пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="snils">Номер СНИЛС.</param>
        /// <param name="insurancePolicyModel">Модель полиса ОМС.</param>
        /// <param name="scopingOrganizationName">Наименование страховой организации.</param>
        private void GeneratePatientInfoTableDataIdentity(string snils, InsurancePolicyModel insurancePolicyModel, string scopingOrganizationName)
        {
            if (!String.IsNullOrWhiteSpace(snils) || insurancePolicyModel is not null)
            {
                this.StreamWriter.WriteLine("               <td class=\"outer\">"); // 4 tabs.
                this.StreamWriter.WriteLine($"                  <strong>СНИЛС: </strong> {snils} <br/>"); // 5 tabs.
                this.StreamWriter.WriteLine("                   <strong>Полис ОМС: </strong><br/>");
                this.StreamWriter.WriteLine($"                  <strong>(Серия) </strong> {insurancePolicyModel.Series} <strong>" +
                                            $"(Номер) </strong> {insurancePolicyModel.Number} ({scopingOrganizationName})");
                this.StreamWriter.WriteLine("               </td>");
            }

            this.ClosePatientInfoTableRowTag();
        }

        /// <summary>
        /// Создает разметку данных документа идентификации пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="documentModel">Модель документа идентификации.</param>
        private void GeneratePatientInfoTableDataIdentityDocument(DocumentModel documentModel)
        {
            if (documentModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("               <td class=\"outer\">"); // 4 tabs.
            this.StreamWriter.WriteLine("                   <strong>Паспорт гражданина Российской Федерации:</strong>"); // 5 tabs.
            if (documentModel.Series is not null)
            {
                this.StreamWriter.Write($"                  <br/>Серия документа: {documentModel.Series}");
            }

            if (documentModel.Number is not null)
            {
                this.StreamWriter.Write($"<br/>Номер документа: {documentModel.Number}");
            }

            if (documentModel.IssueOrgName is not null)
            {
                this.StreamWriter.Write($"<br/>Кем выдан документ: {documentModel.IssueOrgName}");
            }

            if (documentModel.IssueOrgCode is not null)
            {
                this.StreamWriter.Write($"<br/>Код подразделения: {documentModel.IssueOrgCode}");
            }

            this.StreamWriter.WriteLine($"<br/>Дата выдачи: {documentModel.IssueDate.ToString("dd.MM.yyyy")}");
            this.StreamWriter.WriteLine("               </td>");
            
            this.ClosePatientInfoTableRowTag();
        }

        /// <summary>
        /// Создает разметку данных контактной информации пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="permanentAddressModel">Модель адреса постоянной регистрации.</param>
        /// <param name="actualAddressModel">Модель данных фактического адреса проживания.</param>
        /// <param name="contacts">Контакты.</param>
        private void GeneratePatientInfoTableDataContactInfo(
            AddressModel permanentAddressModel,
            AddressModel actualAddressModel, 
            List<TelecomModel> contacts)
        {
            this.StreamWriter.WriteLine("               <td class=\"outer\">"); // 4 tabs.
            
            if (permanentAddressModel is not null)
            {
                this.StreamWriter.WriteLine("                   <strong>Адрес постоянной регистрации: </strong>"); // 5 tabs.
                this.GeneratePatientInfoTableDataContactInfoAddress(permanentAddressModel);
            }
            
            if (actualAddressModel is not null)
            {
                this.StreamWriter.WriteLine("                   <strong>Адрес фактического проживания: </strong>"); // 5 tabs.
                this.GeneratePatientInfoTableDataContactInfoAddress(actualAddressModel);
            }

            if (contacts is not null && contacts.Count != 0)
            {
                this.StreamWriter.WriteLine("                   <strong>Контакты:</strong>"); // 5 tabs.
                var contactsStr = this.GetContactsString(contacts);
                this.StreamWriter.WriteLine($"                   <br/>{contactsStr}<br/>");
            }

            this.StreamWriter.WriteLine("               </td>");
            
            this.ClosePatientInfoTableRowTag();
        }

        /// <summary>
        /// Создает разметку адреса в таблице контактной информации пациента таблицы "Информация о пациенте".
        /// </summary>
        /// <param name="addressModel">Модель адреса.</param>
        private void GeneratePatientInfoTableDataContactInfoAddress(AddressModel addressModel)
        {
            if (addressModel is not null)
            {
                string addressString = String.Empty;
                
                if (addressModel.PostalCode is not null)
                {
                    addressString += addressModel.PostalCode + ", ";
                }

                if (addressModel.StreetAddressLine is not null)
                {
                    addressString += addressModel.StreetAddressLine;
                }

                if (!String.IsNullOrEmpty(addressString))
                {
                    this.StreamWriter.Write($"                  <br/>{addressString}"); // 5 tabs.
                }

                if (addressModel.StateCode.Code is not null)
                {
                    this.StreamWriter.Write($"<strong> Код субъекта РФ: </strong> {addressModel.StateCode.Code}");
                }

                if (addressModel.StateCode.DisplayName is not null)
                {
                    this.StreamWriter.WriteLine($"<text> ( </text> {addressModel.StateCode.DisplayName} <text> ) </text> <br/>");
                }
            }
        }
        
        #endregion

        /// <summary>
        /// Получить возраст пациента.
        /// </summary>
        /// <param name="birthday">Дата рождения.</param>
        /// <returns>Возраст пациента.</returns>
        private string GetPatientAge(DateTime birthday)
        {
            var age = DateTime.Now.Year - birthday.Year;
            if (DateTime.Now.DayOfYear < birthday.DayOfYear) age++;

            return $"{age} {this.GetAgeDeclination(age)}";
        }

        /// <summary>
        /// Получить склонение возраста.
        /// </summary>
        /// <param name="age">Возраст.</param>
        /// <returns>Склонение возраста.</returns>
        private string GetAgeDeclination(int age)
        {
            if (age > 100)
            {
                age = age % 100;
            }
            if (age >= 0 && age <= 20)
            {
                if (age == 0)
                {
                    return "лет";
                }
                else if (age == 1)
                {
                    return "год";
                }
                else if (age >= 2 && age <= 4)
                {
                    return "года";
                }
                else if (age >= 5 && age <= 20)
                {
                    return "лет";
                }
            }
            else if (age > 20)
            {
                string str;
                str = age.ToString();
                string n = str[str.Length - 1].ToString();
                int m = Convert.ToInt32(n);
                if (m == 0)
                {
                    return "лет";
                }
                else if (m == 1)
                {
                    return "год";
                }
                else if (m >= 2 && m <= 4)
                {
                    return "года";
                }
                else
                {
                    return "лет";
                }
            }
            return null;
        }
        
        /// <summary>
        /// Открывает тег строки в таблице "Информация о пациенте".
        /// </summary>
        private void OpenPatientInfoTableRowTag()
        {
            this.StreamWriter.WriteLine("           <tr class=\"outer\">"); // 3 tabs.
        }

        /// <summary>
        /// Закрывает тег строки в таблице "Информация о пациенте".
        /// </summary>
        private void ClosePatientInfoTableRowTag()
        {
            this.StreamWriter.WriteLine("           </tr>");
        }

        #endregion

        #region Sections

        /// <summary>
        /// Создает секцию "Секции".
        /// </summary>
        /// <param name="documentBodyModel">Модель тела документа.</param>
        private void GenerateSectionsTable(DirectionToMSEDocumentModel documentModel)
        {
            if (documentModel is null)
            {
                return;
            }
            var documentBodyModel = documentModel.DocumentBody;
            if (documentBodyModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("       <table class=\"Sections\">"); // 2 tabs.
            this.StreamWriter.WriteLine("           <tbody>"); // 3 tabs.

            this.GenerateSentSection(documentBodyModel.SentSection.SentParagraphs);
            this.GenerateLaborActivitySection(documentBodyModel.WorkplaceSection.WorkPlaceParagraphs);
            this.GenerateEducationSection(documentBodyModel.EducationSection.FillingSection);
            this.GenerateAnamnezSection(documentBodyModel.AnamnezSection);
            this.GenerateVitalParametersSection(documentBodyModel.VitalParametersSection);
            this.GenerateDirectionStateSection(documentBodyModel.DirectionStateSection);
            this.GenerateDiagnosticStudiesSection(documentBodyModel.DiagnosticStudiesSection);
            this.GenerateDiagnosisSection(documentBodyModel.DiagnosisSection);
            this.GenerateConditionAssessmentSection(documentBodyModel.ConditionAssessmentSection);
            this.GenerateRecommendationsSection(documentBodyModel.RecommendationsSection);
            this.GenerateOutsideSpecialMedicalCareSection(documentBodyModel.OutsideSpecialMedicalCareSection);
            this.GenerateAttachmentDocumentsSection(documentBodyModel.AttachmentDocumentsSection);
            
            this.StreamWriter.WriteLine("           </tbody>");
            this.StreamWriter.WriteLine("       </table>");
            this.StreamWriter.WriteLine("       <br/>");
            this.StreamWriter.WriteLine("       <hr/>");

            this.GeneratePerformersSection(documentModel.ServiceEvent.Performer,
                documentModel.ServiceEvent.OtherPerformers);
            this.GenerateAuthorAndLegalAuthenticatorSection(documentModel.Author, documentModel.LegalAuthenticator);
        }
        
        #region Sent section

        /// <summary>
        /// Создает секцию "Направление".
        /// </summary>
        /// <param name="sentParagraphs">Элементы(параграфы) секции "Направление".</param>
        private void GenerateSentSection(List<ParagraphModel> sentParagraphs)
        {
            if (sentParagraphs is null || sentParagraphs.Count == 0)
            {
                return;
            }
            
            this.GenerateSectionTitle("SCOPORG", "НАПРАВЛЕНИЕ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");

            foreach (var paragraph in sentParagraphs)
            {
                this.GenerateSectionsTableDataParagraphElement(paragraph.Caption, paragraph.Content);
            }

            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        #endregion

        #region LaborActivity section

        /// <summary>
        /// Создает секцию "Трудовая деятельность".
        /// </summary>
        /// <param name="laborActivitys">Элементы(параграфы) секции "Трудовая деятельность".</param>
        private void GenerateLaborActivitySection(List<ParagraphModel> laborActivitys)
        {
            if (laborActivitys is null || laborActivitys.Count == 0)
            {
                return;
            }
            
            this.GenerateSectionTitle("WORK", "ТРУДОВАЯ ДЕЯТЕЛЬНОСТЬ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");

            foreach (var paragraph in laborActivitys)
            {
                this.GenerateSectionsTableDataParagraphElement(paragraph.Caption, paragraph.Content);
            }

            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        #endregion

        #region EducationSection

        /// <summary>
        /// Создает секцию "Образование".
        /// </summary>
        /// <param name="educationInformation">Элемент наполнения секции "Образование".</param>
        private void GenerateEducationSection(ParagraphModel educationInformation)
        {
            if (educationInformation is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("EDU", "ОБРАЗОВАНИЕ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            
            this.GenerateSectionsTableDataParagraphElement(educationInformation.Caption, educationInformation.Content);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        #endregion

        #region Anamnez Section

        /// <summary>
        /// Создает секцию "Анамнез".
        /// </summary>
        /// <param name="anamnezSectionModel">Модель секции "Анамнез".</param>
        private void GenerateAnamnezSection(AnamnezSectionModel anamnezSectionModel)
        {
            if (anamnezSectionModel is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("SOCANAM", "АНАМНЕЗ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            
            this.GenerateAnamnezSectionTableData(anamnezSectionModel);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        /// <summary>
        /// Создает элементы таблицы данные секции "Анамнез".
        /// </summary>
        /// <param name="anamnezSectionModel">Модель секции "Анамнез".</param>
        private void GenerateAnamnezSectionTableData(AnamnezSectionModel anamnezSectionModel)
        {
            if (anamnezSectionModel.Disability is not null)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Инвалидность: </span><br/>");
                
                if (anamnezSectionModel.Disability.GroupText is not null)
                {
                    this.StreamWriter.WriteLine($"{anamnezSectionModel.Disability.GroupText}<br/>");
                }

                if (anamnezSectionModel.Disability.TimeDisability is not null)
                {
                    this.StreamWriter.WriteLine($"Находился на инвалидности на момент направления: {anamnezSectionModel.Disability.TimeDisability}<br/>");
                }
                
                this.StreamWriter.WriteLine("                       <p/>");
            }

            if (anamnezSectionModel.DegreeDisability is not null
                && anamnezSectionModel.DegreeDisability.DegreeDisabilities is not null
                && anamnezSectionModel.DegreeDisability.DegreeDisabilities.Count != 0)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Степень утраты профессиональной трудоспособности: </span><br/>");

                foreach (var degreeDisability in anamnezSectionModel.DegreeDisability.DegreeDisabilities)
                {
                    if (degreeDisability.FullText is not null)
                    {
                        this.StreamWriter.WriteLine($"{degreeDisability.FullText}<br/>");
                    }
                }
                
                this.StreamWriter.WriteLine("                       <p/>");
            }
            
            if (anamnezSectionModel.SeenOrganizations is not null)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Наблюдается в организациях, оказывающих лечебно-профилактическую помощь: </span><br/>");
                this.StreamWriter.WriteLine($"{anamnezSectionModel.SeenOrganizations}<br/>");
                this.StreamWriter.WriteLine("                       <p/>");
            }

            if (anamnezSectionModel.MedicalAnamnez is not null)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Анамнез заболевания: </span><br/>");
                this.StreamWriter.WriteLine($"{anamnezSectionModel.MedicalAnamnez}<br/>");
                this.StreamWriter.WriteLine("                       <p/>");
            }

            if (anamnezSectionModel.LifeAnamnez is not null)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Анамнез жизни: </span><br/>");
                this.StreamWriter.WriteLine($"{anamnezSectionModel.LifeAnamnez}<br/>");
                this.StreamWriter.WriteLine("                       <p/>");
            }

            if (anamnezSectionModel.ActualDevelopment is not null)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Физическое развитие (в отношении детей в возрасте до 3 лет): </span><br/>");
                this.StreamWriter.WriteLine($"{anamnezSectionModel.ActualDevelopment}<br/>");
                this.StreamWriter.WriteLine("                       <p/>");
            }
            
            this.GenerateTemporaryDisabilityTable(anamnezSectionModel.TemporaryDisabilitys);
            
            if (anamnezSectionModel.CertificateDisabilityNumber is not null)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Листок нетрудоспособности в форме электронного документа: </span><br/>");
                this.StreamWriter.WriteLine($"{anamnezSectionModel.CertificateDisabilityNumber}<br/>");
                this.StreamWriter.WriteLine("                       <p/>");
            }
            
            if (anamnezSectionModel.EffectityAction is not null && anamnezSectionModel.EffectityAction.Count != 0)
            {
                this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Результаты и эффективность проведенных мероприятий медицинской реабилитации: </span><br/>");
                foreach (var effectityAction in anamnezSectionModel.EffectityAction)   
                {
                    this.StreamWriter.WriteLine($"{effectityAction}<br/>");
                }
                this.StreamWriter.WriteLine("                       <p/>");
            }
        }

        /// <summary>
        /// Создает таблицу "Временная нетрудоспособность" секции "Анамнез".
        /// </summary>
        /// <param name="temporaryDisabilitys">Список моделей элементов "Временная нетрудоспособность".</param>
        private void GenerateTemporaryDisabilityTable(List<TemporaryDisabilityModel> temporaryDisabilitys)
        {
            if (temporaryDisabilitys is null || temporaryDisabilitys.Count == 0)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"100%\">");
            this.StreamWriter.WriteLine("                           <span style=\"font-weight:bold; \">Временная нетрудоспособность: </span>");
            this.StreamWriter.WriteLine("                           <col xmlns=\"urn:hl7-org:v3\" xmlns:fias=\"urn:hl7-ru:fias\" xmlns:medService=\"urn:hl7-ru:medService\" width=\"10%\"/>");
            this.StreamWriter.WriteLine("                           <col xmlns=\"urn:hl7-org:v3\" xmlns:fias=\"urn:hl7-ru:fias\" xmlns:medService=\"urn:hl7-ru:medService\" width=\"20%\"/>");
            this.StreamWriter.WriteLine("                           <col xmlns=\"urn:hl7-org:v3\" xmlns:fias=\"urn:hl7-ru:fias\" xmlns:medService=\"urn:hl7-ru:medService\" width=\"70%\"/>");
            this.StreamWriter.WriteLine("                           <tbody>");
            this.StreamWriter.WriteLine("                               <tr>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Дата начала</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Дата окончания</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Число дней</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Шифр МКБ</th>");
            this.StreamWriter.WriteLine("                               </tr>");
            foreach (var temporaryDisability in temporaryDisabilitys)
            {
                this.GenerateTemporaryDisabilityeTableRowTable(temporaryDisability);
            }
            this.StreamWriter.WriteLine("                           </tbody>");
            this.StreamWriter.WriteLine("                       </table>");
            this.StreamWriter.WriteLine("                       <br/>");
        }

        /// <summary>
        /// Создает строки таблицы "Временная нетрудоспособность" секции "Анамнез".
        /// </summary>
        /// <param name="temporaryDisabilitys">Модель элмента "Временная нетрудоспособность".</param>
        private void GenerateTemporaryDisabilityeTableRowTable(TemporaryDisabilityModel temporaryDisabilitys)
        {
            this.StreamWriter.WriteLine("                               <tr>");
            this.StreamWriter.WriteLine($"                                   <th class=\"inner\" colspan=\"\">{temporaryDisabilitys.DateStart.ToString("dd.MM.yyyy")}</th>");
            this.StreamWriter.WriteLine($"                                   <th class=\"inner\" colspan=\"\">{temporaryDisabilitys.DateFinish.ToString("dd.MM.yyyy")}</th>");
            
            if (temporaryDisabilitys.DayCount is not null)
            {
                this.StreamWriter.WriteLine($"                                   <th class=\"inner\" colspan=\"\">{temporaryDisabilitys.DayCount}</th>");
            }

            if (temporaryDisabilitys.CipherMKB is not null)
            {
                this.StreamWriter.WriteLine($"                                   <th class=\"inner\" colspan=\"\">{temporaryDisabilitys.CipherMKB}</th>");
            }
            
            this.StreamWriter.WriteLine("                               </tr>");
        }
        
        #endregion

        #region Vital parameters section

        /// <summary>
        /// Создает секцию "Витальные параметры".
        /// </summary>
        /// <param name="vitalParametersSectionModel">Модель секции "Витальные параметры".</param>
        private void GenerateVitalParametersSection(VitalParametersSectionModel vitalParametersSectionModel)
        {
            if (vitalParametersSectionModel is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("VITALPARAM", "АНТРОПОМЕТРИЧЕСКИЕ ДАННЫЕ И ФИЗИОЛОГИЧЕСКИЕ ПАРАМЕТРЫ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            
            this.GenerateVitalParametersSectionTable(vitalParametersSectionModel);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        /// <summary>
        /// Создает таблицу секции "Витальные параметры".
        /// </summary>
        /// <param name="vitalParametersSectionModel">Модель секции "Витальные параметры".</param>
        private void GenerateVitalParametersSectionTable(VitalParametersSectionModel vitalParametersSectionModel)
        {
            if (vitalParametersSectionModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"100%\">");
            this.StreamWriter.WriteLine("                           <col xmlns=\"urn:hl7-org:v3\" xmlns:fias=\"urn:hl7-ru:fias\" xmlns:medService=\"urn:hl7-ru:medService\" width=\"30%\"/>");
            this.StreamWriter.WriteLine("                           <col xmlns=\"urn:hl7-org:v3\" xmlns:fias=\"urn:hl7-ru:fias\" xmlns:medService=\"urn:hl7-ru:medService\" width=\"70%\"/>");
            this.StreamWriter.WriteLine("                           <tbody>");

            if (vitalParametersSectionModel.VitalParameters is not null 
                && vitalParametersSectionModel.VitalParameters.Count != 0)
            {
                foreach (var vitalParameter in vitalParametersSectionModel.VitalParameters)
                {
                    this.GenerateVitalParametersSectionTableRow(vitalParameter);
                }
            }

            this.StreamWriter.WriteLine("                                   <td class=\"inner\" colspan=\"\">Телосложение</td>");
            
            if (vitalParametersSectionModel.BodyType is not null)
            {
                this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{vitalParametersSectionModel.BodyType}</td>");
            }

            this.StreamWriter.WriteLine("                           </tbody>");
            this.StreamWriter.WriteLine("                       </table>");
        }

        /// <summary>
        /// Создает строки таблицы секции "Витальные параметры".
        /// </summary>
        /// <param name="vitalParameterModel">Модель витального параметра.</param>
        private void GenerateVitalParametersSectionTableRow(VitalParameterModel vitalParameterModel)
        {
            if (vitalParameterModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                               <tr>");
            
            if (vitalParameterModel.EntryDisplayName is not null)
            {
                this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{vitalParameterModel.EntryDisplayName}</td>");
            }

            if (vitalParameterModel.Value is not null && vitalParameterModel.Unit is not null)
            {
                this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{vitalParameterModel.Value} {vitalParameterModel.Unit}</td>");
            }
            
            this.StreamWriter.WriteLine("                               </tr>");
        }

        #endregion

        #region Direction State Section

        /// <summary>
        /// Создает секцию "Состояние при направлении".
        /// </summary>
        /// <param name="directionStateSectionModel">Модель секции "Состояние при направлении".</param>
        private void GenerateDirectionStateSection(DirectionStateSectionModel directionStateSectionModel)
        {
            if (directionStateSectionModel is null || String.IsNullOrWhiteSpace(directionStateSectionModel.StateText))
            {
                return;
            }
            
            this.GenerateSectionTitle("STATECUR", "СОСТОЯНИЕ ПРИ НАПРАВЛЕНИИ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            
            this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Состояние здоровья гражданина при направлении на медико-социальную экспертизу: </span><br/>");
            this.StreamWriter.WriteLine($"{directionStateSectionModel.StateText}<br/><p/>");
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        #endregion
        
        #region Diagnostic Studies Section

        /// <summary>
        /// Создает секцию "Медецинские обследования".
        /// </summary>
        /// <param name="diagnosticStudiesSectionModel">Модель секции "Медицинские обследования".</param>
        private void GenerateDiagnosticStudiesSection(DiagnosticStudiesSectionModel diagnosticStudiesSectionModel)
        {
            if (diagnosticStudiesSectionModel is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("PROC", "МЕДИЦИНСКИЕ ОБСЛЕДОВАНИЯ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");

            this.GenerateDiagnosticStudiesSectionTable(diagnosticStudiesSectionModel);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        /// <summary>
        /// Создает таблицу секции "Медецинские обследования".
        /// </summary>
        /// <param name="diagnosticStudiesSectionModel">Модель секции "Медицинские обследования".</param>
        private void GenerateDiagnosticStudiesSectionTable(DiagnosticStudiesSectionModel diagnosticStudiesSectionModel)
        {
            if (diagnosticStudiesSectionModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"\">");
            this.StreamWriter.WriteLine("                           <span style=\"font-weight:bold;\">Сведения о медицинских обследованиях, необходимых для получения клинико-функциональных данных в зависимости от заболевания при проведении медико-социальной экспертизы:</span>");
            this.StreamWriter.WriteLine("                           <tbody>");
            
            this.StreamWriter.WriteLine("                               <tr>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Дата</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Код</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Наименование</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Результат</th>");
            this.StreamWriter.WriteLine("                               </tr>");

            this.GenerateDiagnosticStudiesSectionTableRows(diagnosticStudiesSectionModel.MedicalExaminations);
            
            this.StreamWriter.WriteLine("                           </tbody>");
            this.StreamWriter.WriteLine("                       </table>");
        }

        /// <summary>
        /// Создает строки таблицы секции "Медицинское обследование".
        /// </summary>
        /// <param name="medicalExaminations">Список медицинских обследований.</param>
        private void GenerateDiagnosticStudiesSectionTableRows(List<MedicalExaminationModel> medicalExaminations)
        {
            if (medicalExaminations is null || medicalExaminations.Count == 0)
            {
                return;
            }

            foreach (var medicalExamination in medicalExaminations)
            {
                this.StreamWriter.WriteLine("                               <tr>");
                this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{medicalExamination.Date.ToString("dd.MM.yyyy")}</td>");
                
                if (medicalExamination.Number is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{medicalExamination.Number}</td>");
                }

                if (medicalExamination.Name is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{medicalExamination.Name}</td>");
                }

                if (medicalExamination.Result is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{medicalExamination.Result}</td>");
                }
                
                this.StreamWriter.WriteLine("                               </tr>");
            }
        }

        #endregion
        
        #region Diagnosis Section

        /// <summary>
        /// Создает секцию "Диагнозы".
        /// </summary>
        /// <param name="diagnosisSectionModel">Модель секции "Диагнозы".</param>
        private void GenerateDiagnosisSection(DiagnosisSectionModel diagnosisSectionModel)
        {
            if (diagnosisSectionModel is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("DGN", "ДИАГНОЗЫ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");

            this.GenerateGenerateDiagnosisSectionTable(diagnosisSectionModel);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        /// <summary>
        /// Создает таблицу секции "Диагнозы".
        /// </summary>
        /// <param name="diagnosisSectionModel">Модель секции "Диагнозы".</param>
        private void GenerateGenerateDiagnosisSectionTable(DiagnosisSectionModel diagnosisSectionModel)
        {
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"\">");
            this.StreamWriter.WriteLine("                           <span style=\"font-weight:bold;\">Диагноз при направлении на медико-социальную экспертизу:</span>");
            this.StreamWriter.WriteLine("                           <tbody>");
            
            this.StreamWriter.WriteLine("                               <tr>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Шифр</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Тип</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Текст</th>");
            this.StreamWriter.WriteLine("                               </tr>");

            this.GenerateGenerateDiagnosisSectionTableRows(diagnosisSectionModel.Diagnosis);
            
            this.StreamWriter.WriteLine("                           </tbody>");
            this.StreamWriter.WriteLine("                       </table>");
        }

        /// <summary>
        /// Создать строки таблицы секции "Диагнозы".
        /// </summary>
        /// <param name="diagnostics">Список диагнозов.</param>
        private void GenerateGenerateDiagnosisSectionTableRows(List<DiagnosticModel> diagnostics)
        {
            if (diagnostics is null || diagnostics.Count == 0)
            {
                return;
            }

            foreach (var diagnostic in diagnostics)
            {
                this.StreamWriter.WriteLine("                               <tr>");
                if (diagnostic.ID is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{diagnostic.ID}</td>");
                }

                if (diagnostic.Caption is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{diagnostic.Caption}</td>");
                }

                if (diagnostic.Result is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <td class=\"inner\" colspan=\"\">{diagnostic.Result}</td>");
                }
                
                this.StreamWriter.WriteLine("                               </tr>");
            }
        }
        
        #endregion
        
        #region Condition Assessment Section

        /// <summary>
        /// Создает секцию "Объектизированная оценка состояния".
        /// </summary>
        /// <param name="conditionAssessmentSectionModel">Модель секции "Объектизированная оценка состояния".</param>
        private void GenerateConditionAssessmentSection(ConditionAssessmentSection conditionAssessmentSectionModel)
        {
            if (conditionAssessmentSectionModel is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("SCORES", "ОБЪЕКТИВИЗИРОВАННАЯ ОЦЕНКА СОСТОЯНИЯ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");

            this.GenerateConditionAssessmentSectionTable(conditionAssessmentSectionModel);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        /// <summary>
        /// Создает таблицу секции "Объектизированная оценка состояния".
        /// </summary>
        /// <param name="conditionAssessmentSectionModel">Модель секции "Объектизированная оценка состояния".</param>
        private void GenerateConditionAssessmentSectionTable(ConditionAssessmentSection conditionAssessmentSectionModel)
        {
            if (conditionAssessmentSectionModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"\">");

            this.StreamWriter.WriteLine("                           <thead>");
            this.StreamWriter.WriteLine("                               <tr>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Тип оценки</th>");
            this.StreamWriter.WriteLine("                                   <th class=\"inner\">Результат</th>");
            this.StreamWriter.WriteLine("                               </tr>");
            this.StreamWriter.WriteLine("                           <thead>");

            this.StreamWriter.WriteLine("                           <tbody>");
            
            if (conditionAssessmentSectionModel.ClinicalPrognosis is not null)
            {
                this.GenerateConditionAssessmentSectionTableRow(
                    conditionAssessmentSectionModel.ClinicalPrognosis.GrateType,
                    conditionAssessmentSectionModel.ClinicalPrognosis.GrateResult);
            }

            if (conditionAssessmentSectionModel.RehabilitationPotential is not null)
            {
                this.GenerateConditionAssessmentSectionTableRow(
                    conditionAssessmentSectionModel.RehabilitationPotential.GrateType,
                    conditionAssessmentSectionModel.RehabilitationPotential.GrateResult);
            }

            if (conditionAssessmentSectionModel.RehabilitationPrognosis is not null)
            {
                this.GenerateConditionAssessmentSectionTableRow(
                    conditionAssessmentSectionModel.RehabilitationPrognosis.GrateType,
                    conditionAssessmentSectionModel.RehabilitationPrognosis.GrateResult);
            }
            
            this.StreamWriter.WriteLine("                           </tbody>");
            
            this.StreamWriter.WriteLine("                       </table>");
        }

        /// <summary>
        /// Создает строку таблицы секции "Объектизированная оценка состояния".
        /// </summary>
        /// <param name="type">Тип.</param>
        /// <param name="result">Результат.</param>
        private void GenerateConditionAssessmentSectionTableRow(string type, string result)
        {
            this.StreamWriter.WriteLine("                               <tr>");
            if (type is not null)
            {
                this.StreamWriter.WriteLine($"                                  <td class=\"inner\" colspan=\"\">{type}</td>");
            }

            if (result is not null)
            {
                this.StreamWriter.WriteLine($"                                  <td class=\"inner\" colspan=\"\">{result}</td>");
            }
            
            this.StreamWriter.WriteLine("                               </tr>");
        }

        #endregion
        
        #region Condition Assessment Section

        /// <summary>
        /// Создает секцию "Рекомендации".
        /// </summary>
        /// <param name="recommendationsSectionModel">Модель секции "Рекомендации".</param>
        private void GenerateRecommendationsSection(RecommendationsSectionModel recommendationsSectionModel)
        {
            if (recommendationsSectionModel is null)
            {
                return;
            }
            
            this.GenerateSectionTitle("REGIME", "РЕКОМЕНДАЦИИ");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\">");
            this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\" title=\"RECTREAT\">Рекомендованное лечение</span>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");

            this.GenerateRecommendationsSectionTable(recommendationsSectionModel);
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");

            this.GenerateRecommendationsSectionOtherRecommendatons(recommendationsSectionModel.OtherRecommendatons);
        }

        /// <summary>
        /// Создает таблицу секции "Рекомендации".
        /// </summary>
        /// <param name="recommendationsSectionModel">Модель секции "Рекомендации".</param>
        private void GenerateRecommendationsSectionTable(RecommendationsSectionModel recommendationsSectionModel)
        {
            if (recommendationsSectionModel is null)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"\">");
            this.StreamWriter.WriteLine("                           <tbody>");

            if (recommendationsSectionModel.RecommendedMeasuresReconstructiveSurgery is not null)
            {
                this.GenerateRecommendationsSectionTableRow(
                    "Рекомендуемые мероприятия по реконструктивной хирургии",
                    recommendationsSectionModel.RecommendedMeasuresReconstructiveSurgery);
            }
            
            if (recommendationsSectionModel.RecommendedMeasuresProstheticsAndOrthotics is not null)
            {
                this.GenerateRecommendationsSectionTableRow(
                    "Рекомендуемые мероприятия по протезированию и ортезированию, техническим средствам реабилитации",
                    recommendationsSectionModel.RecommendedMeasuresProstheticsAndOrthotics);
            }
            
            if (recommendationsSectionModel.SpaTreatment is not null)
            {
                this.GenerateRecommendationsSectionTableRow(
                    "Санаторно-курортное лечение",
                    recommendationsSectionModel.SpaTreatment);
            }

            this.GenerateRecommendationsSectionMedicationTableRow(recommendationsSectionModel.Medications);

            if (recommendationsSectionModel.MedicalDevices is not null)
            {
                this.GenerateRecommendationsSectionTableRow(
                    "Перечень медицинских изделий для медицинского применения",
                    recommendationsSectionModel.MedicalDevices);
            }
            
            this.StreamWriter.WriteLine("                           </tbody>");
            this.StreamWriter.WriteLine("                       </table>");
        }

        /// <summary>
        /// Создает строку таблицы секции "Рекомендации".
        /// </summary>
        /// <param name="caption">Описание.</param>
        /// <param name="value">Значение.</param>
        private void GenerateRecommendationsSectionTableRow(string caption, string value)
        {
            this.StreamWriter.WriteLine("                               <tr>");
            if (caption is not null)
            {
                this.StreamWriter.WriteLine($"                                  <td class=\"inner\" colspan=\"\">{caption}</td>");
            }

            if (value is not null)
            {
                this.StreamWriter.WriteLine($"                                  <td class=\"inner\" colspan=\"\">{value}</td>");
            }

            this.StreamWriter.WriteLine("                               </tr>");
        }

        /// <summary>
        /// Создает строку таблицы секции "Рекомендации" с перечнем лекарств.
        /// </summary>
        /// <param name="medications">Перечень лекарств.</param>
        private void GenerateRecommendationsSectionMedicationTableRow(List<MedicationModel> medications)
        {
            if (medications is null || medications.Count == 0)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("                                   <td class=\"inner\" colspan=\"\">");
            this.StreamWriter.WriteLine("Перечень лекарственных препаратов для медицинского применения (заполняется в отношении граждан, пострадавших в результате несчастных случаев на производстве и профессиональных заболеваний)");
            this.StreamWriter.WriteLine("                                   </td>");
            
            this.StreamWriter.WriteLine("                                   <td class=\"inner\" colspan=\"\">");
            foreach (var medication in medications)
            {
                this.StreamWriter.WriteLine($"{this.GenerateValueMedicationTableRow(medication)}");
            }
            this.StreamWriter.WriteLine("                                   </td>");
        }

        /// <summary>
        /// Создает струку с информацией о лекарстве.
        /// </summary>
        /// <param name="medicationModel">Модель лекарства.</param>
        /// <returns></returns>
        private string GenerateValueMedicationTableRow(MedicationModel medicationModel)
        {
            string resultString = String.Empty;
            
            if (medicationModel.InternationalName is not null)
            {
                resultString += $"Международное название:<br/> {medicationModel.InternationalName} <br/> ";
            }

            if (medicationModel.InternationalName is not null)
            {
                resultString += $"Лекарственная форма:<br/> {medicationModel.DosageForm} <br/> ";
            }

            if (medicationModel.Dose is not null)
            {
                resultString += $"Лекарственная доза:<br/> {medicationModel.Dose} <br/> ";
            }

            if (medicationModel.KTRUCode is not null)
            {
                resultString += $"Код КТРУ:<br/> {medicationModel.KTRUCode} <br/> ";
            }

            if (medicationModel.DurationAdmission is not null)
            {
                resultString += $"Продолжительность приема:<br/> {medicationModel.DurationAdmission} <br/> ";
            }

            if (medicationModel.MultiplicityCoursesTreatment is not null)
            {
                resultString += $"Кратность курсов лечения:<br/> {medicationModel.MultiplicityCoursesTreatment} <br/> ";
            }

            if (medicationModel.ReceptionFrequency is not null)
            {
                resultString += $"Кратность приема: <br/> {medicationModel.ReceptionFrequency} <br/> ";
            }

            resultString += "<p/>";

            return resultString;
        }

        /// <summary>
        /// Создает подсекцию "Прочие рекомендации" секции "Рекомендации".
        /// </summary>
        /// <param name="otherRecommendatons">Текст поля "Прочие рекомендации".</param>
        private void GenerateRecommendationsSectionOtherRecommendatons(string otherRecommendatons)
        {
            if (String.IsNullOrWhiteSpace(otherRecommendatons))
            {
                return;
            }
            
            this.StreamWriter.WriteLine("               <tr><td colspan=\"2\"><br/></td></tr>");
            this.StreamWriter.WriteLine("               <tr><td><br/></td></tr>");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\">");
            this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\" title=\"RECOTHER\">Прочие рекомендации</span>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            this.StreamWriter.WriteLine("                       <span style=\"font-weight:bold;\">Рекомендуемые мероприятия по медицинской реабилитации: </span>");
            this.StreamWriter.WriteLine($"                       <br/>{otherRecommendatons}<br/>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               <tr><td colspan=\"2\"><br/></td></tr>");
        }
        
        #endregion

        #region Outside Special Medical Care Section

        /// <summary>
        /// Создает секцию "Посторонний специальный медицинский уход".
        /// </summary>
        /// <param name="outsideSpecialMedicalCareSectionModel">Модель секции "Посторонний специальный медицинский уход".</param>
        private void GenerateOutsideSpecialMedicalCareSection(OutsideSpecialMedicalCareSectionModel outsideSpecialMedicalCareSectionModel)
        {
            if (outsideSpecialMedicalCareSectionModel is null
                || String.IsNullOrWhiteSpace(outsideSpecialMedicalCareSectionModel.Text))
            {
                return;
            }
            
            this.GenerateSectionTitle("OUTSPECMEDCARE", "ПОСТОРОННИЙ СПЕЦИАЛЬНЫЙ МЕДИЦИНСКИЙ УХОД");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            
            this.StreamWriter.WriteLine($"{outsideSpecialMedicalCareSectionModel.Text} <p/>");
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        #endregion

        #region Attachment documents

        /// <summary>
        /// Создает секцию прикрепленых документов обследования.
        /// </summary>
        /// <param name="attachmentDocumentsSectionModel">Модель секции прикрепленных документов обследования.</param>
        private void GenerateAttachmentDocumentsSection(AttachmentDocumentsSectionModel attachmentDocumentsSectionModel)
        {
            if (attachmentDocumentsSectionModel is null
                || attachmentDocumentsSectionModel.AttachmentDocuments is null
                || attachmentDocumentsSectionModel.AttachmentDocuments.Count == 0)
            {
                return;
            }
            
            this.GenerateSectionTitle("LINKDOCS", "Связанные документы");
            
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SubSectionTitle\" />");
            this.StreamWriter.WriteLine("                   <td class=\"Rest\">");
            
            this.StreamWriter.WriteLine("                       <table class=\"inner\" width=\"\">");
            this.StreamWriter.WriteLine("                           <tbody>");
            foreach (var attachmentDocument in attachmentDocumentsSectionModel.AttachmentDocuments)
            {
                this.StreamWriter.WriteLine("                               <tr>");
                
                if (attachmentDocument.Name is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <th class=\"inner\">{attachmentDocument.Name}</th>");
                }

                if (attachmentDocument.Result is not null)
                {
                    this.StreamWriter.WriteLine($"                                   <th class=\"inner\">{attachmentDocument.Result}</th>");
                }

                this.StreamWriter.WriteLine("                               </tr>");
            }
            this.StreamWriter.WriteLine("                           </tbody>");
            this.StreamWriter.WriteLine("                       </table>");
            
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }

        #endregion

        #region Performers

        /// <summary>
        /// Создает секцию "Участники врачебной комисии".
        /// </summary>
        /// <param name="performer">Модель председателя врачебной комисии.</param>
        /// <param name="otherPerformers">Список моделей участников врачебной комисии.</param>
        private void GeneratePerformersSection(PerformerModel performer, List<PerformerModel> otherPerformers)
        {
            this.StreamWriter.WriteLine("       <table class=\"outer\" width=\"100%\">");
            this.StreamWriter.WriteLine("           <col width=\"40%\"/>");
            this.StreamWriter.WriteLine("           <col width=\"60%\"/>");

            this.GeneratePerformerSection(performer);

            this.GenerateOtherPerformersSection(otherPerformers);
            
            this.StreamWriter.WriteLine("       </table>");
        }

        /// <summary>
        /// Генерирует разметку для председателя комиссии.
        /// </summary>
        /// <param name="performerModel">Модель председателя комисии.</param>
        private void GeneratePerformerSection(PerformerModel performerModel)
        {
            if (performerModel is null)
            {
                return;
            }

            string nameString = String.Empty;

            if (performerModel.Position is not null)
            {
                if (performerModel.Position.DisplayName is not null)
                {
                    nameString += performerModel.Position.DisplayName + " ";
                }
            }

            if (performerModel.Name is not null)
            {
                if (performerModel.Name.Family is not null)
                {
                    nameString += performerModel.Name.Family + " ";
                }
                if (performerModel.Name.Given is not null)
                {
                    nameString += performerModel.Name.Given + " ";
                }
                if (performerModel.Name.Patronymic is not null)
                {
                    nameString += performerModel.Name.Patronymic + " ";
                }
            }
            
            this.StreamWriter.WriteLine("           <tr class=\"outer\">");
            this.StreamWriter.WriteLine("               <th class=\"outer\" width=\"20%\">Председатель комиссии:</th>");
            this.StreamWriter.WriteLine($"               <td class=\"outer\">{nameString}<br/>");
            this.StreamWriter.WriteLine("           </tr>");
        }

        /// <summary>
        /// Генерирует разметку для других участников комисии.
        /// </summary>
        /// <param name="otherPerformers">Список моделей участников комисии.</param>
        private void GenerateOtherPerformersSection(List<PerformerModel> otherPerformers)
        {
            if (otherPerformers is null
                || otherPerformers.Count == 0)
            {
                return;
            }
            
            this.StreamWriter.WriteLine("           <tr class=\"outer\">");
            this.StreamWriter.WriteLine("               <th class=\"outer\" width=\"20%\">Члены врачебной комиссии:</th>");
            
            foreach (var performer in otherPerformers)
            {
                string nameString = String.Empty;
                
                if (performer.Position is not null)
                {
                    if (performer.Position.DisplayName is not null)
                    {
                        nameString += performer.Position.DisplayName + " ";
                    }
                }

                if (performer.Name is not null)
                {
                    if (performer.Name.Family is not null)
                    {
                        nameString += performer.Name.Family + " ";
                    }
                    if (performer.Name.Given is not null)
                    {
                        nameString += performer.Name.Given + " ";
                    }
                    if (performer.Name.Patronymic is not null)
                    {
                        nameString += performer.Name.Patronymic + " ";
                    }
                }

                this.StreamWriter.WriteLine($"               <td class=\"outer\"><strong>- </strong>{nameString}<br/>");
            }
            
            this.StreamWriter.WriteLine("           </tr>");
        }

        #endregion

        #region Author and Legal authenticator

        /// <summary>
        /// Создает секции "Автор и лицо, придавшее юридическую силу документу".
        /// </summary>
        /// <param name="author">Модель автора доумента.</param>
        /// <param name="legalAuthenticatorModel">Модель лица, придавшего юридическую силу документу.</param>
        private void GenerateAuthorAndLegalAuthenticatorSection(
            AuthorDataModel author,
            LegalAuthenticatorModel legalAuthenticatorModel)
        {
            if (author is null || legalAuthenticatorModel is null)
            {
                return;
            }
            this.StreamWriter.WriteLine("       <br/>");
            this.StreamWriter.WriteLine("       <hr/>");
            this.StreamWriter.WriteLine("       <table class=\"outer\" width=\"100%\">");
            this.StreamWriter.WriteLine("           <col width=\"40%\"/>");
            this.StreamWriter.WriteLine("           <col width=\"60%\"/>");

            this.GenerateAutorSection(author);
            this.GenerateLegalAuthenticator(legalAuthenticatorModel);
            
            this.StreamWriter.WriteLine("       </table>");
        }

        /// <summary>
        /// Создает разметку для автора документа.
        /// </summary>
        /// <param name="authorDataModel">Модель автора документа.</param>
        private void GenerateAutorSection(AuthorDataModel authorDataModel)
        {
            if (authorDataModel is null)
            {
                return;
            }

            string nameString = String.Empty;
            if (authorDataModel.Author.Position is not null)
            {
                if (authorDataModel.Author.Position.DisplayName is not null)
                {
                    nameString += authorDataModel.Author.Position.DisplayName + " ";
                }
            }
            
            if (authorDataModel.Author.Name is not null)
            {
                if (authorDataModel.Author.Name.Family is not null)
                {
                    nameString += authorDataModel.Author.Name.Family + " ";
                }
                if (authorDataModel.Author.Name.Given is not null)
                {
                    nameString += authorDataModel.Author.Name.Given + " ";
                }
                if (authorDataModel.Author.Name.Patronymic is not null)
                {
                    nameString += authorDataModel.Author.Name.Patronymic + " ";
                }
            }
            
            var authorContacts = authorDataModel.Author.Contacts;
            authorContacts.Add(authorDataModel.Author.ContactPhoneNumber);

            string authorString = "<strong>Должность, ФИО: </strong> " +
                                  $"<br/> {nameString} <br/> " +
                                  "<strong>Контакты: </strong>" +
                                  $"{this.GetContactsString(authorContacts)}";
            
            this.StreamWriter.WriteLine("           <tr class=\"outer\">");
            this.StreamWriter.WriteLine("               <th class=\"outer\" width=\"20%\">Документ составил:</th>");
            this.StreamWriter.WriteLine($"               <td class=\"outer\">{authorString}<br/>");
            this.StreamWriter.WriteLine("           </tr>");
        }

        /// <summary>
        /// Создает разметку для лица, придавшего юридическую силу документу.
        /// </summary>
        /// <param name="legalAuthenticatorModel">Модель лица, придавшего юридическую силу документу.</param>
        private void GenerateLegalAuthenticator(LegalAuthenticatorModel legalAuthenticatorModel)
        {
            string nameString = String.Empty;
            if (legalAuthenticatorModel.AssignedEntity.Position is not null)
            {
                if (legalAuthenticatorModel.AssignedEntity.Position.DisplayName is not null)
                {
                    nameString += legalAuthenticatorModel.AssignedEntity.Position.DisplayName + " ";
                }
            }
            
            if (legalAuthenticatorModel.AssignedEntity.Name is not null)
            {
                if (legalAuthenticatorModel.AssignedEntity.Name.Family is not null)
                {
                    nameString += legalAuthenticatorModel.AssignedEntity.Name.Family + " ";
                }
                if (legalAuthenticatorModel.AssignedEntity.Name.Given is not null)
                {
                    nameString += legalAuthenticatorModel.AssignedEntity.Name.Given + " ";
                }
                if (legalAuthenticatorModel.AssignedEntity.Name.Patronymic is not null)
                {
                    nameString += legalAuthenticatorModel.AssignedEntity.Name.Patronymic + " ";
                }
            }
            
            var authorContacts = legalAuthenticatorModel.AssignedEntity.Contacts;
            authorContacts.Add(legalAuthenticatorModel.AssignedEntity.ContactPhoneNumber);

            string authorString = "<strong>Должность, ФИО: </strong> " +
                                  $"<br/> {nameString} <br/> " +
                                  "<strong>Контакты: </strong>" +
                                  $"{this.GetContactsString(authorContacts)}";
            
            this.StreamWriter.WriteLine("           <tr class=\"outer\">");
            this.StreamWriter.WriteLine("               <th class=\"outer\" width=\"20%\">Документ заверил:</th>");
            this.StreamWriter.WriteLine($"               <td class=\"outer\">{authorString}<br/>");
            this.StreamWriter.WriteLine("           </tr>");
        }
        
        #endregion
        
        /// <summary>
        /// Создает блок с наименование секции.
        /// </summary>
        /// <param name="title">Title секции.</param>
        /// <param name="name">Наименование секции.</param>
        private void GenerateSectionTitle(string title, string name)
        {
            if (String.IsNullOrWhiteSpace(title) || String.IsNullOrWhiteSpace(name))
            {
                return;
            }
            
            this.StreamWriter.WriteLine("               <tr>"); // 4 tabs.
            this.StreamWriter.WriteLine("                   <td colspan=\"2\">"); // 5 tabs.
            this.StreamWriter.WriteLine("                       <br/>"); // 6 tabs.
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td class=\"SectionTitle\" colspan=\"2\">");
            this.StreamWriter.WriteLine($"                       <span style=\"font-weight:bold;\" title=\"{title}\"> {name}</span>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
            this.StreamWriter.WriteLine("               <tr>");
            this.StreamWriter.WriteLine("                   <td colspan=\"2\">");
            this.StreamWriter.WriteLine("                       <br/>");
            this.StreamWriter.WriteLine("                   </td>");
            this.StreamWriter.WriteLine("               </tr>");
        }
        
        /// <summary>
        /// Создает элемент (параграфы) секций.
        /// </summary>
        /// <param name="name">Имя элемента.</param>
        /// <param name="values">Значение элемента.</param>
        private void GenerateSectionsTableDataParagraphElement(string name, List<string> values)
        {
            if (values is null || values.Count == 0 || String.IsNullOrWhiteSpace(name))
            {
                return;
            }
            
            this.StreamWriter.WriteLine($"                       <span style=\"font-weight:bold;\">{name}: </span>"); // 6 tabs.
            foreach (var value in values)
            {
                this.StreamWriter.WriteLine("                       <br/>");
                this.StreamWriter.WriteLine($"                           {value}");
                this.StreamWriter.WriteLine("                       <br/>");
            }
            this.StreamWriter.WriteLine("                       <p/>");
        }
        
        #endregion

        /// <summary>
        /// Создает строку "Контакты".
        /// </summary>
        /// <param name="contacts">Список контактов.</param>
        /// <returns>Строка "Контакты".</returns>
        private string GetContactsString(List<TelecomModel> contacts)
        {
            if (contacts is null || contacts.Count == 0)
            {
                return string.Empty;
            }
            
            string resultString = string.Empty;
            string email = string.Empty;
            string emailPost = string.Empty;
            string mobilePhone = string.Empty;
            string fax = string.Empty;
            string phone = string.Empty;

            foreach (var contact in contacts)
            {
                if (contact.Value.Contains("http"))
                {
                    email = contact.Value;
                }
                else if (contact.Value.Contains("+79"))
                {
                    mobilePhone = contact.Value;
                }
                else if (contact.Value.Contains("+7"))
                {
                    phone = contact.Value;
                }
                else if (contact.Value.Contains("@"))
                {
                    emailPost = contact.Value;
                }
                else
                {
                    fax = contact.Value;
                }
            }

            if (!String.IsNullOrEmpty(mobilePhone))
            {
                resultString += $"Тел.: {mobilePhone}; ";
            }

            if (!String.IsNullOrEmpty(fax))
            {
                resultString += $"Факс: {fax}; ";
            }

            if (!String.IsNullOrEmpty(email))
            {
                resultString += $"{email}; ";
            }

            if (!String.IsNullOrEmpty(phone))
            {
                resultString += $"Тел.: {phone}; ";
            }

            if (!String.IsNullOrEmpty(emailPost))
            {
                resultString += $"Электронная почта: {emailPost}; ";
            }
            
            return resultString;
        }
        
        /// <summary>
        /// Начало документа.
        /// Открывает тег "html".
        /// </summary>
        private void OpenHtmlTag()
        {
            this.StreamWriter.WriteLine("<html xmlns:n1=\"urn:hl7-org:v3\" " +
                                        "xmlns:n2=\"urn:hl7-org:v3/meta/voc\" " +
                                        "xmlns:voc=\"urn:hl7-org:v3/voc\" " +
                                        "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
                                        "xmlns:identity=\"urn:hl7-ru:identity\" " +
                                        "xmlns:address=\"urn:hl7-ru:address\">");
        }

        /// <summary>
        /// Конец документа.
        /// Закрываем тег "html".
        /// </summary>
        private void CloseHtmlTag()
        {
            this.StreamWriter.WriteLine("</html>");
            this.StreamWriter.Close();
        }
    }
}