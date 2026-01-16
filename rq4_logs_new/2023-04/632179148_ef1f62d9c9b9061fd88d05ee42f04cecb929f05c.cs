using System;
using AutoMapper;
using DoorApi.ViewModels;
using Dto;
using Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace DoorApi.Controllers
{
	public class DoorController
	{
		private readonly IDoorService _doorService;
		private readonly IIotGatewayService _iotGatewayService;
		private readonly IMapper _mapper;

		public DoorController(IDoorService doorService, IIotGatewayService iotGatewayService, IMapper mapper)
		{
			_doorService = doorService;
            _iotGatewayService = iotGatewayService;
			_mapper = mapper;
		}

		public bool Open(DoorViewModel viewModel)
		{
			var dto = _mapper.Map<DoorDto>(viewModel);

			if (!_doorService.ValidOpen(dto))
			{
				return false;
			}
		}
    }
}
