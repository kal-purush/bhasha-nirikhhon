using Avalonia.Markup.Xaml.Templates;
using GroupDocs.Editor;
using GroupDocs.Editor.Formats;
using GroupDocs.Editor.HtmlCss.Resources;
using GroupDocs.Editor.Options;
using Microsoft.Extensions.Options;
using NickBuhro.NumToWords.Russian;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Legion.Helpers.ReportGenerator
{
    internal class WordGenerator
    {
        public void GenerateDocument(Legion.Models.Contract cntr, string documentType)
        {
            Legion.Models.Contract contract = cntr;

            string TemplateName = "";

            switch (documentType)
            {
                case "Закрытие договора":
                {
                    TemplateName = "заявление на закрытие договора инвестирования";
                    break;
                }
                case "Акт":
                {
                    TemplateName = "акт";
                    break;
                }
                case "Доп соглашение":
                {
                    TemplateName = "доп соглашение";
                    break;
                }
                case "Пролонгация":
                {
                    TemplateName = "пролонгация";
                    break;
                }
                case "Договор инвестирования":
                {
                    TemplateName = "договор инвестирования";
                    break;
                }
                case "Договор накопительный":
                {
                    TemplateName = "договор накопительный";
                    break;
                }
            }

            using (FileStream fs = File.OpenRead($"Шаблон {TemplateName}.docx"))
            {  
                WordProcessingLoadOptions loadOptions = new WordProcessingLoadOptions();
                using (Editor editor = new Editor(delegate { return fs; }, delegate { return loadOptions; }))
                {
                    WordProcessingEditOptions editOptions = new WordProcessingEditOptions
                    {
                        FontExtraction = FontExtractionOptions.ExtractEmbeddedWithoutSystem,
                        EnableLanguageInformation = true,
                        EnablePagination = true
                    };

                    using (EditableDocument beforeEdit = editor.Edit(editOptions))
                    {
                        string originalContent = beforeEdit.GetContent();
                        List<IHtmlResource> allResources = beforeEdit.AllResources;

                        string editedContent = "";

                        switch (documentType)
                        {
                            case "Закрытие договора":
                            {
                                editedContent = originalContent
                                    .Replace("Иванов Иван Иванович", $"{contract.Investor.LastName} {contract.Investor.FirstName} {contract.Investor.MiddleName}")
                                    .Replace("111/20", $"{contract.CustomId}")
                                    .Replace("01.01.2020", contract.DateStart.ToString("dd.MM.yyyy"))
                                    .Replace("100 000 (сто тысяч)", contract.Amount.ToString("### ### ###") + $" ({RussianConverter.Format(contract.Amount)})");
                                break;
                            }
                            case "Акт":
                            {
                                TemplateName = $"Акт №{contract.CustomId} {contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.";
                                CultureInfo culture = new CultureInfo("ru-RU");
                                editedContent = originalContent
                                    .Replace("18/21 от 12 августа 2021", $"{contract.CustomId} от {contract.DateStart.Day} {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year}")
                                    .Replace("18/21", $"{contract.CustomId}")
                                    .Replace("Садовников Сергей Васильевич", $"{contract.Investor.LastName} {contract.Investor.FirstName} {contract.Investor.MiddleName}")
                                    .Replace("125 000 (сто двадцать пять тысяч)", contract.Amount.ToString("### ### ###") + $" ({RussianConverter.Format(contract.Amount)})")
                                    .Replace("Садовников С. В.", $"{contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.");
                                break;
                            }
                            case "Доп соглашение":
                            {
                                //доделать
                                TemplateName = $"Доп соглашение от {contract.DateStart.ToString("dd.MM.yyyy")}";
                                CultureInfo culture = new CultureInfo("ru-RU");
                                editedContent = originalContent
                                    //.Replace("03/21 от «10» февраля 2021 г", $"{contract.CustomId} от «{contract.DateStart.Day}» {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year} г.")
                                    .Replace("«10» марта 2021 г.", $"«{contract.DateStart.Day}» {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year} г.")
                                    .Replace("Гладышева Наталья Леонидовна", $"{contract.Investor.LastName} {contract.Investor.FirstName} {contract.Investor.MiddleName}");
                                    //.Replace("70 000 (семьдесят тысяч) ", contract.Amount.ToString("### ### ###") + $" ({RussianConverter.Format(contract.Amount)})")
                                    break;
                            }
                            case "Пролонгация":
                            {
                                //доделать
                                TemplateName = $"Доп соглашение к договору от {contract.DateStart.ToString("dd.MM.yyyy")} {contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.";
                                CultureInfo culture = new CultureInfo("ru-RU");
                                editedContent = originalContent
                                    .Replace("86 от «12» июля 2019 г.", $"{contract.CustomId} от «{contract.DateStart.Day}» {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year} г.")
                                    .Replace("«10» марта 2021 г.", $"«{contract.DateStart.Day}» {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year} г.")
                                    .Replace("Матвеева Татьяна Викторовна", $"{contract.Investor.LastName} {contract.Investor.FirstName} {contract.Investor.MiddleName}");
                                //.Replace("70 000 (семьдесят тысяч) ", contract.Amount.ToString("### ### ###") + $" ({RussianConverter.Format(contract.Amount)})")
                                break;
                            }
                            case "Договор инвестирования":
                            {
                                TemplateName = $"Договор инвестирования №{contract.CustomId} {contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.";
                                CultureInfo culture = new CultureInfo("ru-RU");
                                editedContent = originalContent
                                    .Replace("598/21", $"{contract.CustomId}")
                                    .Replace("«11» августа 2021  г.", $"«{contract.DateStart.Day}» {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year} г.")
                                    .Replace("Осипов Артем Владимирович", $"{contract.Investor.LastName} {contract.Investor.FirstName} {contract.Investor.MiddleName}")
                                    .Replace("«11» августа 2022 г.", $"«{contract.DateEnd.Day}» {contract.DateEnd.ToString("MMMM", culture)} {contract.DateEnd.Year} г.")
                                    .Replace("700 000 (семьсот тысяч)", contract.Amount.ToString("### ### ###") + $" ({RussianConverter.Format(contract.Amount)})")
                                    .Replace("11 числа", $"{contract.DateEnd.Day} числа")
                                    .Replace("5% (пять процентов)", $"{contract.Bet}% ({RussianConverter.FormatCurrency((decimal)contract.Bet)})")
                                    .Replace("35 000 (тридцать пять тысяч)", $"{(contract.Amount * contract.Bet / 100).ToString("### ### ###")} ({RussianConverter.FormatCurrency((decimal)(contract.Amount * contract.Bet / 100))})")
                                    .Replace("29.04.1984", contract.Investor.DateBirth.ToString("dd.MM.yyyy"))
                                    .Replace("0404", $"{contract.Investor.PassprotSeries}")
                                    .Replace("785967", $"{contract.Investor.PassprotNumber}")
                                    .Replace("Управлением внутренних дел города Минусинска и Минусинского района Красноярского края", $"{contract.Investor.Given}")
                                    .Replace("21.12.2004", contract.Investor.PassportDateGiven.ToString("dd.MM.yyyy"))
                                    .Replace("242-039", $"{contract.Investor.PassportUnitCode}")
                                    .Replace("Красноярский край, г. Минусинск, ул. Подгорная, д. 80, кв. 3", $"{contract.Investor.PassportRegistration}")
                                    .Replace("89134470961", $"{contract.Investor.Phone}")
                                    .Replace("Осипов А. В.", $"{contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.");
                                break;
                            }
                            case "Договор накопительный":
                            {
                                TemplateName = $"Договор накопительный {contract.CustomId} {contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.";
                                CultureInfo culture = new CultureInfo("ru-RU");
                                editedContent = originalContent
                                    .Replace("18/21", $"{contract.CustomId}")
                                    .Replace("«12» августа 2021 г.", $"«{contract.DateStart.Day}» {contract.DateStart.ToString("MMMM", culture)} {contract.DateStart.Year} г.")
                                    .Replace("Садовников Сергей Васильевич", $"{contract.Investor.LastName} {contract.Investor.FirstName} {contract.Investor.MiddleName}")
                                    .Replace("«12» февраля 2022 г.", $"«{contract.DateEnd.Day}» {contract.DateEnd.ToString("MMMM", culture)} {contract.DateEnd.Year} г.")
                                    .Replace("125 000 (сто двадцать пять тысяч)", contract.Amount.ToString("### ### ###") + $" ({RussianConverter.Format(contract.Amount)})")
                                    .Replace("02.02.1969", contract.Investor.DateBirth.ToString("dd.MM.yyyy"))
                                    .Replace("0415", $"{contract.Investor.PassprotSeries}")
                                    .Replace("815228", $"{contract.Investor.PassprotNumber}")
                                    .Replace("Отделом УФМС России по Красноярскому краю в Октябрьском р-не г. Красноярска ", $"{contract.Investor.Given}")
                                    .Replace("02.09.2015", contract.Investor.PassportDateGiven.ToString("dd.MM.yyyy"))
                                    .Replace("240-006", $"{contract.Investor.PassportUnitCode}")
                                    .Replace("г. Красноярск, ул.Курчатова, д. 15Б, кв. 95", $"{contract.Investor.PassportRegistration}")
                                    .Replace("89831475675", $"{contract.Investor.Phone}")
                                    .Replace("Садовников С. В.", $"{contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}.");
                                    break;
                            }
                        }
                        
                        using (EditableDocument afterEdit = EditableDocument.FromMarkup(editedContent, allResources))
                        {
                            WordProcessingFormats docxFormat = WordProcessingFormats.Docx;
                            WordProcessingSaveOptions saveOptions = new WordProcessingSaveOptions(docxFormat);

                            using (FileStream outputStream = File.Create($"{TemplateName}.docx"))
                            {
                                editor.Save(afterEdit, outputStream, saveOptions);
                            }
                        }
                    }
                }
            }
        }
    }
}