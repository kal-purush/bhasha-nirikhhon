using System;
using System.Collections.Generic;
using System.Linq;
using ApplicationLayer.Interfaces;
using Unity.VisualScripting;
using ApplicationLayer.Dtos.JB;
using ApplicationLayer.Dtos.Grapper;
using ApplicationLayer.Dtos.AdapterSpecification;
using ApplicationLayer.Dtos.Rack;
using System.Threading.Tasks;
using UnityEngine.UI;
using ApplicationLayer.Dtos.Company;
public class LoginPresenter
{
    private readonly ILoginView _view;
    private readonly ICompanyService _companyService;
    private readonly IGrapperService _grapperService;

    public LoginPresenter(
        ILoginView view,
        ICompanyService CompanyService,
        IGrapperService GrapperService)
    {
        _view = view;
        _companyService = CompanyService;
        _grapperService = GrapperService;
    }

    //! Get list Login chỉ có Id và Code
    public async void LoginProcess()
    {
        GlobalVariable.APIRequestType.Add("GET_Company");
        GlobalVariable.APIRequestType.Add("GET_Grapper_List");
        
        _view.ShowLoading("Đang đăng nhập...");

        try
        {
            var companyTask = _companyService.GetCompanyByIdAsync(1);
            var grapperTask = _grapperService.GetListGrapperAsync(1);

            await Task.WhenAll(companyTask, grapperTask);

            var companyDto = companyTask.Result;
            var grapperDtos = grapperTask.Result;

            if (companyDto == null || grapperDtos == null)
            {
                _view.ShowError("Tải dữ liệu thất bại!");
                return;
            }

            if (companyDto != null)
            {
                var models = ConvertCompanyModelFromDto(companyDto);
                if (models != null)
                {
                    GlobalVariable.temp_CompanyInformationModel = models;
                }
            }
            else
            {
                _view.ShowError("Tải dữ liệu thất bại!");
                return;
            }
            if (grapperDtos != null)
            {
                var models = new List<GrapperInformationModel>();
                if (grapperDtos.Any())
                {
                    models = grapperDtos.Select(dto => ConvertGrapperFromResponseDto(dto)).ToList();
                }
                if (models != null)
                {
                    GlobalVariable.temp_List_GrapperInformationModel = models;
                }
            }
            else
            {
                _view.ShowError("Tải dữ liệu thất bại!");
                return;
            }

            _view.ShowSuccess(); // Chỉ hiển thị thành công nếu result == true
        }
        catch (Exception)
        {
            _view.ShowError("Tải dữ liệu thất bại!");
        }
        finally
        {
            _view.HideLoading();
            GlobalVariable.APIRequestType.Remove("GET_Company");
            GlobalVariable.APIRequestType.Remove("GET_Grapper_List");
        }
    }

    private CompanyInformationModel ConvertCompanyModelFromDto(CompanyResponseDto dto)
    {
        return new CompanyInformationModel(
            id: dto.Id,
            name: dto.Name,
            listGrapperInformationModel: dto.GrapperBasicDtos.Select(
                grapper => new GrapperInformationModel(
                    id: grapper.Id,
                    name: grapper.Name
                )).ToList(),
            listModuleSpecificationModel: dto.ModuleSpecificationBasicDtos.Select(
                module => new ModuleSpecificationModel(
                    id: module.Id,
                    code: module.Code
                )).ToList(),
            listAdapterSpecificationModel: dto.AdapterSpecificationBasicDtos.Select(
                adapter => new AdapterSpecificationModel(
                    id: adapter.Id,
                    code: adapter.Code
                )).ToList()
        );
    }
    //! Dto => Model
    private GrapperInformationModel ConvertGrapperFromResponseDto(GrapperBasicDto dto)
    {
        return new GrapperInformationModel(
            id: dto.Id,
            name: dto.Name);
    }
}