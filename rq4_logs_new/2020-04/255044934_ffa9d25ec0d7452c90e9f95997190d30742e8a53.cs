using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.JSInterop;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using PecMembers.UI.Data;
using PecMembers.UI.Data.PecMemberModels;
using PecMembers.UI.Repositories.GenericRepoForPecMembers.PecMembersCurrentRepo;
using PecMembers.UI.Services;
using PecMembers.UI.ViewModel;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace PecMembers.UI.Pages.PecMembersTec
{
    public class PecMembersViewBase:ComponentBase
    {
        [Inject]
        protected IJSRuntime jJSRuntime { get; set; }

        [Inject]
        protected RoleManager<IdentityRole> roleManager { get; set; }
        [Inject]
        protected UserManager<ApplicationUser> userManager { get; set; }
        public ApplicationUser user { get; set; }
        [Parameter]
        public string userName { get; set; } = string.Empty;

        [CascadingParameter]
        private Task<AuthenticationState> authenticationStateTask { get; set; }

        [Inject]
        protected IPecMembersCurrentRepos pecMembersCurrentRepos { get; set; }
        public PecMembersCurrent pecMembersCurrent { get; set; }
        public List<PecMembersCurrent> pecMembersCurrentList { get; set; } = new List<PecMembersCurrent>();
        public List<PecMemberViewModel> pecMemberViewModelList { get; set; }
        public List<PecMemberViewModel> filteredPecMemberViewModelList { get; set; }

        [Parameter]
        public string SerchColumType1 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType2 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType3 { get; set; } = string.Empty;[Parameter]
        public string SerchColumType4 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType5 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType6 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType7 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType8 { get; set; } = string.Empty;
        [Parameter]
        public string SerchColumType9 { get; set; } = string.Empty;

        //used to store state of screen
        protected string Message = string.Empty;
        protected string StatusClass = string.Empty;
        protected bool Show = false;
        protected override async Task OnInitializedAsync()
        {
            userName = await GetPartyName();
            InitializedPecMember();
            if (userName != "RoleAdmin")
            {
                pecMemberViewModelList = InitializedPecMemberViewModel().Where(p => p.PartyView == userName).ToList();
            }
            else
            {
                pecMemberViewModelList = InitializedPecMemberViewModel().Where(p => p.PartyView.Contains("ԸԸՀ")).ToList();
            }

            filteredPecMemberViewModelList = pecMemberViewModelList;
            await base.OnInitializedAsync();
        }

        private void InitializedPecMember()
        {
            pecMembersCurrentList = pecMembersCurrentRepos.GetAll().ToList();
        }

        private async Task<string> GetPartyName()
        {
            string partyN = string.Empty;
            var authState = await authenticationStateTask;
            var user1 = authState.User;
            user = await userManager.GetUserAsync(user1);
            if (await userManager.IsInRoleAsync(user, "Admin"))
            {
                partyN = "RoleAdmin";
            }
            else
            {
                partyN = user.PName;
            }
            return partyN;
        }

        public List<PecMemberViewModel> InitializedPecMemberViewModel()
        {
            CultureInfo culture = new CultureInfo("hy-AM");
            List<PecMemberViewModel> pecMemberViewModelList = new List<PecMemberViewModel>();

            foreach (var item in pecMembersCurrentList)
            {
                PecMemberViewModel pecMemberViewModel = new PecMemberViewModel()
                {
                    Id = item.Id,
                    ElectionDayView = item.ElectionDay.ToString("dd.MM.yyyy"),
                    DistrictView = item.DistrictId.ToString() != null ? item.DistrictId.ToString() : "",
                    SubDistrictCodeView = item.SubDistrictCode.ToString() != null ? item.SubDistrictCode.ToString() : "",
                    CommunityView = item.Name != null ? item.Name : "",
                    FullName = item.LastName + " " + item.FirstName + " " + item.MiddleName,
                    CerteficateView = item.Certeficate != null ? item.Certeficate : "",
                    PhoneNumberView = item.PhoneNumber != null ? item.PhoneNumber : "",
                    PartyView = item.PartyName != null ? item.PartyName : "",
                    PositionView = item.WorkPosition != null ? item.WorkPosition : "",
                };
                pecMemberViewModelList.Add(pecMemberViewModel);
            }
            return pecMemberViewModelList;

        }

        public void OnPublisherSearchTextChanged(ChangeEventArgs changeEventArgs, string columnTitle)
        {
            string searchText = changeEventArgs.Value.ToString();
            // filteredPecMemberViewModelList = pecMemberViewModelList.Where(p => p.PartyView.Contains(searchText)).ToList();

            switch (columnTitle)
            {
                case "Ամսաթիվ":
                    SerchColumType1 = searchText;
                    break;
                case "ԸԸՀ":
                    SerchColumType2 = searchText;
                    break;
                case "ՏԸՀ":
                    SerchColumType3 = searchText;
                    break;
                case "Համայնք":
                    SerchColumType4 = searchText;
                    break;
                case "Անուն,Ազգանուն,Հայրանուն":
                    SerchColumType5 = searchText;
                    break;
                case "Վկայական":
                    SerchColumType6 = searchText;
                    break;
                case "Հեռախոս":
                    SerchColumType7 = searchText;
                    break;
                case "Կուսակ․":
                    SerchColumType8 = searchText;
                    break;
                case "Պաշտոն":
                    SerchColumType9 = searchText;
                    break;
                default:
                    Console.WriteLine("Default case");
                    break;
            }

            filteredPecMemberViewModelList = pecMemberViewModelList.Where(p => (p.ElectionDayView.Contains(SerchColumType1))
                                                                        && (p.DistrictView.Contains(SerchColumType2))
                                                                        && (p.SubDistrictCodeView.Contains(SerchColumType3))
                                                                        && (p.CommunityView.Contains(SerchColumType4))
                                                                        && (p.FullName.Contains(SerchColumType5))
                                                                        && (p.CerteficateView.Contains(SerchColumType6))
                                                                        && (p.PhoneNumberView.Contains(SerchColumType7))
                                                                        && (p.PartyView.Contains(SerchColumType8))
                                                                        && (p.PositionView.Contains(SerchColumType9)))
                                                                            .ToList();
        }

        public void Clear()
        {
            SerchColumType1 = string.Empty;
            SerchColumType2 = string.Empty;
            SerchColumType3 = string.Empty;
            SerchColumType4 = string.Empty;
            SerchColumType5 = string.Empty;
            SerchColumType6 = string.Empty;
            SerchColumType7 = string.Empty;
            SerchColumType8 = string.Empty;
            SerchColumType9 = string.Empty;
            filteredPecMemberViewModelList = pecMemberViewModelList;
        }


        public void DownloadExcel()
        {
            byte[] fileContents;
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            using (var package = new ExcelPackage())

            {
                //////// --------Start  Excel Style Part --------
                var workSheet = package.Workbook.Worksheets.Add("Sheet1");
                int rowNumber = 9 + filteredPecMemberViewModelList.Count();

                #region TituleForDownloadExcel




                workSheet.Cells["A1:F1"].Merge = true;
                workSheet.Cells["A2:F2"].Merge = true;
                workSheet.Cells["A3:F3"].Merge = true;
                workSheet.Cells["A4:F4"].Merge = true;
                workSheet.Cells["A5:F5"].Merge = true;
                workSheet.Cells["A6:F6"].Merge = true;
                workSheet.Cells["A6:F6"].Merge = true;
                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Merge = true;
                workSheet.Cells[$"A{rowNumber + 1}:C{rowNumber + 1}"].Merge = true;
                workSheet.Cells[$"E{rowNumber + 2}:F{rowNumber + 2}"].Merge = true;
                workSheet.Cells[$"E{rowNumber + 3}:F{rowNumber + 3}"].Merge = true;
                workSheet.Cells[1, 1].Value = "Լրացման կարգ. Աղյուսակի բոլոր վանդակներում տեղեկությունները լրացվում են GHEA Grapalat տառատեսակով, չափը` 10: Տվյալները լրացվում են ԿԸՀ համացանցային կայքից ներբեռնված ֆայլի կրկնօրինակի վրա:Աղյուսակում տողեր ավելացվում են աղյուսակի վերջին տողից առաջ` նշելով այդ տողի բոլոր վանդակները և ընտրելով Անվանացանկի(Menu) Ներմուծել(Insert) բաժնից Տողեր(Rows) կետը:(կուսակցության, կուսակցությունների դաշինքի անվանումը) դաշտում(կուսակցության, կուսակցությունների դաշինքի անվանումը) բառերի փոխարեն լրացվում է կուսակցության, կուսակցությունների դաշինքի անվանումը  Ընտրական տեղամասի N դաշտում լրացվում է ընտրական տեղամասի համարը, որտեղ m - ը համապատասխան ընտրատարածքի համարն է, իսկ n - ը՝ ընտրական տեղամասի հերթական համարը(միանիշ թվերը նշվում են միանիշ):Համայնք սյունակում լրացվում է համայնքի անվանումը(միայն ՏԻՄ ընտրության դեպքում) ՏԸՀ անդամի ազգանուն,անուն, հայրանուն սյունակում լրացվում է ՏԸՀ անդամի ազգանունը, անունը, հայրանունը Որակավորման վկայականի համար սյունակում լրացվում է որակավորման վկայականի համարը Հեռախոսահամար, կապի այլ միջոցներ սյունակում լրացվում է հեռախոսահամարը, կապի այլ միջոցները Ծանոթություն սյունակում լրացվում է ՏԸՀ անդամի պաշտոնը՝ նախագահ, քարտուղար կամ անդամ: ";
                workSheet.Cells[2, 1].Value = "ՀՀ ԿԵՆՏՐՈՆԱԿԱՆ ԸՆՏՐԱԿԱՆ ՀԱՆՁՆԱԺՈՂՈՎԻ ՆԱԽԱԳԱՀԻՆ";
                workSheet.Cells[3, 1].Value = "ՀԱՅՏ";
                //  workSheet.Cells[4, 1].Value = "(կուսակցության, կուսակցությունների դաշինքի անվանումը)";
                workSheet.Cells[4, 1].Value = user.UName;
                workSheet.Cells[5, 1].Value = "տեղամասային ընտրական հանձնաժողովների անդամների նշանակման";
                workSheet.Cells[6, 1].Value = "ՀՀ ընտրական օրենսգրքի 44-րդ հոդվածին համապատասխան __________________________________ ընտրություններին տեղամասային ընտրական հանձնաժողովների անդամներ են նշանակվել.";
                workSheet.Cells[rowNumber, 1].Value = "Հայտում նշված քաղաքացիներն ունեն տեղամասային ընտրական հանձնաժողովում ընդգրկվելու իրավունք, նրանց վրա չեն տարածվում ՀՀ ընտրական օրենսգրքի 39-րդ հոդվածով ընտրական հանձնաժողովի անդամ նշանակվելու համար նախատեսված սահմանափակումները:";
                workSheet.Cells[rowNumber + 1, 1].Value = "Կուսակցության ղեկավար (տեղակալ)Դաշինքի դեպքում` խմբակցության ղեկավար(տեղակալ)";
                workSheet.Cells[rowNumber + 1, 4].Value = "(ազգանուն, անուն)";
                workSheet.Cells[rowNumber + 1, 5].Value = "(ստորագրություն)";
                workSheet.Cells[rowNumber + 2, 5].Value = "«____» _____________     20     թ.";






                workSheet.Cells["A1:F1"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A1:F1"].Style.Font.Size = 10;
                workSheet.Cells["A1:F1"].Style.WrapText = true;
                workSheet.Cells["A1:F1"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A1:F1"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;

                workSheet.Cells["A2:F2"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A2:F2"].Style.Font.Size = 10;
                workSheet.Cells["A2:F2"].Style.WrapText = true;
                workSheet.Cells["A2:F2"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A2:F2"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;


                workSheet.Cells["A3:F3"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A3:F3"].Style.Font.Size = 10;
                workSheet.Cells["A3:F3"].Style.WrapText = true;
                workSheet.Cells["A3:F3"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A3:F3"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;

                workSheet.Cells["A4:F4"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A4:F4"].Style.Font.Size = 10;
                workSheet.Cells["A4:F4"].Style.WrapText = true;
                workSheet.Cells["A4:F4"].Style.Font.Bold = true;
                workSheet.Cells["A4:F4"].Style.Font.Color.Indexed = 4;
                workSheet.Cells["A4:F4"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A4:F4"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;

                workSheet.Cells["A5:F5"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A5:F5"].Style.Font.Size = 10;
                workSheet.Cells["A5:F5"].Style.WrapText = true;
                workSheet.Cells["A5:F5"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A5:F5"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;

                workSheet.Cells["A6:F6"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A6:F6"].Style.Font.Size = 10;
                workSheet.Cells["A6:F6"].Style.WrapText = true;
                workSheet.Cells["A6:F6"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A6:F6"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;

                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Style.Font.Size = 9;
                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Style.WrapText = true;
                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Left;
                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;
                workSheet.Cells[$"A{rowNumber}:F{rowNumber}"].Style.Font.Bold = true;

                workSheet.Cells[$"A{rowNumber + 1}:F{rowNumber + 1}"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells[$"A{rowNumber + 1}:F{rowNumber + 1}"].Style.Font.Size = 9;
                workSheet.Cells[$"A{rowNumber + 1}:F{rowNumber + 1}"].Style.WrapText = true;
                workSheet.Cells[$"A{rowNumber + 1}:F{rowNumber + 1}"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells[$"A{rowNumber + 1}:F{rowNumber + 1}"].Style.VerticalAlignment = ExcelVerticalAlignment.Bottom;
                workSheet.Cells[$"A{rowNumber + 1}:F{rowNumber + 1}"].Style.Font.Bold = true;


                workSheet.Cells["A7:B7"].Merge = true;
                workSheet.Cells["C7:C8"].Merge = true;
                workSheet.Cells["D7:D8"].Merge = true;
                workSheet.Cells["E7:E8"].Merge = true;
                workSheet.Cells["F7:F8"].Merge = true;
                workSheet.Cells["G7:G8"].Merge = true;


                workSheet.Cells["A7:G7"].Style.WrapText = true;
                workSheet.Cells["A7:G7"].Style.Font.Size = 8;
                workSheet.Cells["A7:G7"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A7:G7"].Style.Font.Bold = true;
                //workSheet.Cells["C7:G7"].AutoFilter = true;
                //workSheet.Cells["C8:G8"].AutoFilter = true;
                workSheet.Cells["A7:G7"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A7:G7"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;


                workSheet.Cells["A8:B8"].Style.WrapText = true;
                workSheet.Cells["A8:B8"].Style.Font.Size = 8;
                workSheet.Cells["A8:B8"].Style.Font.Name = "GHEA Grapalat";
                workSheet.Cells["A8:B8"].Style.Font.Bold = true;
                workSheet.Cells["A7:G7"].AutoFilter = true;
                workSheet.Cells["A8:B8"].Style.HorizontalAlignment = ExcelHorizontalAlignment.Center;
                workSheet.Cells["A8:B8"].Style.VerticalAlignment = ExcelVerticalAlignment.Center;

                //////// --------End  Excel Style Part --------

                workSheet.Cells[7, 1].Value = "Ընտրական տեղամաս N";
                workSheet.Cells[8, 1].Value = "m";
                workSheet.Cells[8, 2].Value = "n";
                workSheet.Cells[7, 3].Value = "Համայնք (նշվում է ՏԻՄ ընտրության դեպքում)";
                workSheet.Cells[7, 4].Value = "ՏԸՀ անդամի ազգանուն,անուն, հայրանուն";
                workSheet.Cells[7, 5].Value = "Որակավորման վկայականի համար";
                workSheet.Cells[7, 6].Value = "Հեռախոսահամար, կապի այլ միջոցներ";
                workSheet.Cells[7, 7].Value = "Ծանոթություն";


                workSheet.Row(1).Height = 220;
                workSheet.Row(2).Height = 20;
                workSheet.Row(3).Height = 33;
                workSheet.Row(4).Height = 27;
                workSheet.Row(5).Height = 21;
                workSheet.Row(6).Height = 52;
                workSheet.Row(7).Height = 35;
                workSheet.Row(rowNumber).Height = 45;
                workSheet.Row(rowNumber + 1).Height = 67;

                workSheet.Column(2).Width = 6;
                workSheet.Column(2).Width = 6;
                workSheet.Column(3).Width = 18;
                workSheet.Column(4).Width = 32;
                workSheet.Column(5).Width = 16;
                workSheet.Column(6).Width = 20;
                workSheet.Column(7).Width = 12;

                #endregion

                int i = 1;
                foreach (var item in filteredPecMemberViewModelList)
                {

                    workSheet.Cells[i + 8, 1].Value = item.DistrictView;
                    workSheet.Cells[i + 8, 2].Value = item.SubDistrictCodeView;
                    workSheet.Cells[i + 8, 3].Value = item.CommunityView;
                    workSheet.Cells[i + 8, 4].Value = item.FullName;
                    workSheet.Cells[i + 8, 5].Value = item.CerteficateView;
                    workSheet.Cells[i + 8, 6].Value = item.PhoneNumberView;
                    workSheet.Cells[i + 8, 7].Value = item.PositionView;

                    i++;
                }

                fileContents = package.GetAsByteArray();

            }

            DownloadExcel obj = new DownloadExcel();
            obj.GenerateExcel(jJSRuntime, fileContents);
        }

        public async Task Delete(PecMemberViewModel pecMemeber)
        {
            var pecMembersDeleted = pecMembersCurrentRepos.GetAll().FirstOrDefault(p => p.Id == pecMemeber.Id);
            try
            {
                await pecMembersCurrentRepos.DeleteAsync(pecMembersDeleted);

                StatusClass = "alert-success";
                Message = "Հաջողությամբ հեռացվեցին, թարմացրեք էջը ";
            }
            catch (Exception ex)
            {

                StatusClass = "alert-danger";
                Message = ex.Message;
            }
        }
    }
}