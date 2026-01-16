using AutoMapper;
using Microsoft.CodeAnalysis.Elfie.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Online_Survey.DTOs.Company;
using Online_Survey.Helper;
using Online_Survey.Models;
using Online_Survey.Services;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Online_Survey.Container
{
    public class CompanyServices : ICompanyServices
    {
  
        private readonly InternshipOnlineSurveyContext context;
        private readonly IMapper mapper;
       
        public CompanyServices(InternshipOnlineSurveyContext context, IMapper mapper)
        {
            this.context = context;
            this.mapper = mapper;
           
        }

        public async Task<APIResponse> Create(CompanyDto data)
        {
            APIResponse response = new APIResponse();
            try
            {
              

                Company _company = this.mapper.Map<CompanyDto, Company>(data);
                await this.context.Companies.AddAsync(_company);
                await this.context.SaveChangesAsync();



                response.ResponseCode = 201;
                response.Result = data.Name;

            }
            catch (DbUpdateException ex)
            {
               
               
                if (ex.InnerException != null)
                {
                    response.ErrorMsg = ex.InnerException.Message;
                   
                }

                response.ResponseCode = 400;
                response.ErrorMsg = ex.Message;
            }
            catch (Exception ex)
            {
                response.ResponseCode = 400;
                response.ErrorMsg = ex.Message;


            }
            return response;
        }

        public async Task<List<CompanyDto>> Getall()
        {

            List<CompanyDto> _response = new List<CompanyDto>();
            var _data = await this.context.Companies.ToListAsync();

            if (_data != null)
            {
                _response = this.mapper.Map<List<Company>, List<CompanyDto>>(_data);
            }


            return _response;
        }

        public async Task<CompanyDto> GetbyCode(int id)
        {
            CompanyDto _response = new CompanyDto();
            var _data = await this.context.Companies.FindAsync(id);

            if (_data != null)
            {
                _response = this.mapper.Map<Company, CompanyDto>(_data);
            }


            return _response;
        }

        public async Task<APIResponse> Remove(int id)
        {
            APIResponse response = new APIResponse();
            try
            {

                var _company = await this.context.Companies.FindAsync(id);
                if (_company != null)
                {
                    this.context.Companies.Remove(_company);
                    await this.context.SaveChangesAsync();
                    response.ResponseCode = 200;
                    response.Result = "";
                }
                else
                {
                    response.ResponseCode = 404;
                    response.ErrorMsg = "Data Not Found";
                }


            }
            catch (Exception ex)
            {
                response.ResponseCode = 400;
                response.ErrorMsg = ex.Message;

            }
            return response;
        }

        public async Task<APIResponse> Update(CompanyDto data, int id)
        {
            APIResponse response = new APIResponse();
            try
            {

                var _company = await this.context.Companies.FindAsync(id);
                if (_company != null)
                {
                    _company.Name = data.Name;
                   

                    await this.context.SaveChangesAsync();
                    response.ResponseCode = 200;
                    response.Result = "";
                }
                else
                {
                    response.ResponseCode = 404;
                    response.ErrorMsg = "Data Not Found";
                }


            }
            catch (Exception ex)
            {
                response.ResponseCode = 400;
                response.ErrorMsg = ex.Message;

            }
            return response;
        }
    }
}