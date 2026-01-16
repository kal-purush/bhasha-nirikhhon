using AutoMapper;
using Common.Dtos;
using EMS.Application.DTOs.EM;
using EMS.Domain.Filters.EMS;
using EMS.Domain.Models.EM;
using EMS.Domain.Repositories.EM;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EMS.Application.Services.EM
{
    public class DepartmentService : IDepartmentService
    {
        private readonly IDepartmentRepository _departmentRepository;
        private readonly IMapper _mapper;

        public DepartmentService(IDepartmentRepository departmentRepository, IMapper mapper)
        {
            _departmentRepository = departmentRepository ?? throw new ArgumentNullException(nameof(departmentRepository));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        #region Create
        public async Task<DepartmentResponseDto> CreateDepartmentAsync(DepartmentRequestDto departmentRequestDto)
        {
            if (departmentRequestDto == null)
                throw new ArgumentNullException(nameof(departmentRequestDto));

            Department department = _mapper.Map<Department>(departmentRequestDto);

            if (await _departmentRepository.ExistsAsync(d => d.DepartmentName == department.DepartmentName))
            {
                throw new ArgumentException("A department with the same name already exists");
            }

            await _departmentRepository.AddAsync(department);
            return _mapper.Map<DepartmentResponseDto>(department);
        }
        #endregion

        #region Delete
        public async Task<bool> DeleteDepartmentAsync(string id)
        {
            var department = await _departmentRepository.GetByIdAsync(id);
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            await _departmentRepository.DeleteAsync(department);
            return true;
        }
        #endregion

        #region Get by Id
        public async Task<DepartmentResponseDto> GetDepartmentByIdAsync(string id)
        {
            var department = await _departmentRepository.GetByIdAsync(id);
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            return _mapper.Map<DepartmentResponseDto>(department);
        }
        #endregion

        #region Get Paged
        public async Task<PagedDto<DepartmentResponseDto>> GetPagedDepartmentsAsync(DepartmentFilter filter)
        {
            if (filter == null)
                throw new ArgumentNullException(nameof(filter));

            var pagedDepartments = await _departmentRepository.GetPagedAsync(filter);

            var departmentResponseDtos = _mapper.Map<IEnumerable<DepartmentResponseDto>>(pagedDepartments.Items);

            return new PagedDto<DepartmentResponseDto>(
                departmentResponseDtos,
                pagedDepartments.TotalCount,
                pagedDepartments.PageIndex,
                pagedDepartments.PageSize
            );
        }
        #endregion

        #region Update
        public async Task<DepartmentResponseDto> UpdateDepartmentAsync(string id, DepartmentRequestDto departmentRequestDto)
        {
            if (departmentRequestDto == null)
                throw new ArgumentNullException(nameof(departmentRequestDto));

            var department = await _departmentRepository.GetByIdAsync(id);
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            _mapper.Map(departmentRequestDto, department);

            if (await _departmentRepository.ExistsAsync(d => d.DepartmentName == department.DepartmentName && d.Id != id))
            {
                throw new ArgumentException("A department with the same name already exists");
            }

            await _departmentRepository.UpdateAsync(department);
            return _mapper.Map<DepartmentResponseDto>(department);
        }
        #endregion
    }
}