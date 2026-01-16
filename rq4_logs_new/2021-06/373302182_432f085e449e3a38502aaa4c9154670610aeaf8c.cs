using System;
using System.Collections.Generic;
using System.Linq;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using NeutrackAPI.Data;
using NeutrackAPI.DTOs;
using NeutrackAPI.Models;
using NeutrackAPI.Helpers;
using NeutrackAPI.Data.IRepositories;
using Microsoft.AspNetCore.Authorization;
using System.Security.Claims;

namespace NeutrackAPI.Controllers
{
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class NutritionistsController : ControllerBase
    {
        private readonly IUserRepository _userRepository;
        private readonly IRoleRepository _roleRepository;
        private readonly INutritionistRepository _nutritionistRepository;
        private readonly IMapper _mapper;

        public NutritionistsController(IUserRepository userRepository,
            IRoleRepository roleRepository,
            INutritionistRepository nutritionistRepository,
            IMapper mapper)
        {
            _userRepository = userRepository;
            _roleRepository = roleRepository;
            _nutritionistRepository = nutritionistRepository;
            _mapper = mapper;
        }

        /// <summary>
        /// Adds a new patient to the nutritionist list
        /// </summary>
        /// <param name="userCreateDTO"></param>
        /// <returns></returns>
        [Authorize(Roles = Roles.Nutritionist)]
        [HttpPost, Route("addpatient")]
        public ActionResult<UserReadDTO> AddPatient(PatientCreateDTO userCreateDTO)
        {
            try
            {
                var currentUserId = int.Parse(User.Identity.Name);
                var currentNutritionistId = int.Parse(User.FindFirstValue(ClaimTypes.Spn));
                if (!User.IsInRole(Roles.Nutritionist))
                {
                    return Forbid();
                }
                var patientModel = _mapper.Map<Patient>(userCreateDTO);
                var userModel = _mapper.Map<User>(userCreateDTO);
                var roleModel = _roleRepository.GetRoleByName(Roles.User);
                var existingUser = _userRepository.GetUserByEmail(userModel.Email);
                if (existingUser != null)
                {
                    return UnprocessableEntity("Email has been taken");
                }
                patientModel.User = userModel;
                patientModel.NutritionistId = currentNutritionistId;
                patientModel.NutritionistPatientHistories = new List<NutritionistPatientHistory>
                {
                    new NutritionistPatientHistory
                    {
                        NutritionistId = currentNutritionistId,
                        Patient = patientModel
                    }
                };
                userModel.UserRoles = new List<UserRole>
                {
                    new UserRole
                    {
                        User = userModel,
                        Role = roleModel,
                    },
                };
                userModel.Patient = patientModel;
                _userRepository.CreateUser(userModel);
                _userRepository.SaveChanges();
                var userReadDTO = _mapper.Map<UserReadDTO>(userModel);
                return CreatedAtRoute(nameof(UsersController.GetUserById), new { userReadDTO.Id }, userReadDTO);

            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [Authorize(Roles = Roles.Nutritionist)]
        [HttpGet]
        [Route("{id}/patients")]
        public ActionResult<IEnumerable<PatientReadDTO>> GetPatients(int id)
        {
            try
            {
                var currentUserId = int.Parse(User.Identity.Name);
                var currentNutritionistId = int.Parse(User.FindFirstValue(ClaimTypes.Spn));
                if (!User.IsInRole(Roles.Nutritionist))
                {
                    return Forbid();
                }
                if (currentNutritionistId != id)
                {
                    return Forbid();
                }
                var patients = _nutritionistRepository.GetAllNutritionistPatients(currentNutritionistId);
                return Ok(_mapper.Map<IEnumerable<PatientReadDTO>>(patients));

            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.InnerException.Message);
            }
        }

        [Authorize(Roles = Roles.Nutritionist)]
        [HttpGet]
        [Route("{id}/patients/{patientId}")]
        public ActionResult<PatientReadDTO> GetPatientById(int id, int patientId)
        {
            try
            {
                var currentUserId = int.Parse(User.Identity.Name);
                var currentNutritionistId = int.Parse(User.FindFirstValue(ClaimTypes.Spn));
                if (!User.IsInRole(Roles.Nutritionist))
                {
                    return Forbid();
                }
                if (currentNutritionistId != id)
                {
                    return Forbid();
                }
                var patient = _nutritionistRepository.GetNutritionistPatientById(patientId, currentNutritionistId);
                return Ok(_mapper.Map<PatientReadDTO>(patient));

            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.InnerException.Message);
            }
        }

        /// <summary>
        /// GET api/users/{id}
        /// </summary>
        /// <param name="id"></param>
        /// <returns>A user matching the id parameter</returns>
        [AllowAnonymous]
        [HttpGet("{id}", Name = "GetNutritionistById")]
        public ActionResult<NutritionistReadDTO> GetNutritionistById(int id)
        {
            try
            {
                var currentUserId = int.Parse(User.Identity.Name);
                if (id == currentUserId)
                {
                    return Forbid();
                }

                var nutritionistItem = _nutritionistRepository.GetNutritionistById(id);
                if (nutritionistItem == null)
                {
                    return NotFound();
                }
                return Ok(_mapper.Map<NutritionistReadDTO>(nutritionistItem));

            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }

        }
    }

 
}