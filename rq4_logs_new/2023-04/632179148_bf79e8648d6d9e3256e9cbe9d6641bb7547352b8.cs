using System;
using Interfaces;
using ViewModels;
using Microsoft.AspNetCore.Mvc;
using AutoMapper;
using Dto;

namespace UserApi.Controllers
{
    [ApiController]
    [Route("user")]
    public class RoleController
	{
		IRoleService _roleService;
		IMapper _mapper;
		
		public RoleController(IRoleService roleService, IMapper mapper)
		{
			_roleService = roleService;
			_mapper = mapper;
		}

        [HttpPost("create")]
        public bool CreateRole(RoleViewModel roleViewModel)
		{
			var roleDto = _mapper.Map<RoleDto>(roleViewModel);
			var result = _roleService.CreateRole(roleDto);

			return result;
		}

        [HttpPost("assign")]
        public bool AssignRole(UserInfoRoleViewModel userRoleViewModel)
		{
            var userRoleDto = _mapper.Map<UserInfoRoleDto>(userRoleViewModel);
            var result = _roleService.AssignRole(userRoleDto);

            return result;
        }

        [HttpPost("deassign")]
        public bool DeassignRole(UserInfoRoleViewModel userRoleViewModel)
		{
            var userRoleDto = _mapper.Map<UserInfoRoleDto>(userRoleViewModel);
            var result = _roleService.DeassignRole(userRoleDto);

            return result;
        }
    }
}
