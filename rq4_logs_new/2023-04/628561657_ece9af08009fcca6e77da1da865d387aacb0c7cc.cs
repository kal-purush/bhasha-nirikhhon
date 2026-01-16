using JKEY_INTERNAL.Models;
using JKEY_INTERNAL.Models.CustomModel;
using JKEY_INTERNAL.Services;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Data.Entity;

namespace JKEY_INTERNAL.Controllers
{
    [ApiController]
    [Route("api/Auth")]
    public class AuthController : Controller
    {
        #region Feild
        private readonly ILogger<DeviceController> _logger;
        private readonly JkeyInternalContext _context;

        private IUserService _userService;

        #endregion

        #region Constructor
        public AuthController(
            IUserService userService,
            ILogger<DeviceController> logger, 
            JkeyInternalContext dbContext

            )
        {
            _userService = userService;
            _logger = logger;
            _context = dbContext;
        }

        #endregion


        //public IActionResult Index()
        //{
        //    return View();
        //}

        [HttpGet]
        public IActionResult Register()
        {
            var user =  _context.Users.OrderByDescending(x => x.UserCreated).ToList();
            if (user == null)
            {
                return StatusCode(StatusCodes.Status400BadRequest, user);

            }
            else
            {
                return StatusCode(StatusCodes.Status200OK, user);

            }
        }


            /// <summary>
            /// Đăng ký
            /// </summary>
            /// <param name="registerModel">thông tin người dùng đăng ký</param>
            /// <returns></returns>
            [HttpPost("Register")]
        public  IActionResult Register([FromBody] User registerModel)
        {
            try
            {
                //if (ModelState.IsValid)//kiểm tra dữ liệu truyền vào
                //{//nếu hợp lệ

                var checkUserName =  _context.Users.FirstOrDefault(x => x.UserName == registerModel.UserName);

                    if (checkUserName != null)
                    {
                        return StatusCode(StatusCodes.Status400BadRequest, "Username already existed");
                    }
                    else
                    {
                        //gọi đến service
                        var result =  _userService.RegisterUser(registerModel);
                        return StatusCode(StatusCodes.Status201Created, result);

                    //if (result.Success)//thành công
                    //    {
                    //        return StatusCode(StatusCodes.Status201Created, result);
                    //    }
                    //    else//thất bại
                    //    {
                    //        return StatusCode(StatusCodes.Status400BadRequest, result);
                    //    }
                    }
                  
                //}
                //else//dữ liệu đầu ào không hợp lệ
                //{
                //    return StatusCode(StatusCodes.Status400BadRequest, "Dữ liệu đầu vào không hợp lệ");
                //}
            }
            catch (Exception ex)
            {                               
                //trả về trạng thái lỗi cho client và mã lỗi cho hàm handle
                return StatusCode(StatusCodes.Status500InternalServerError, ex);

            }
        }



        /// <summary>
        /// Đăng nhập
        /// </summary>
        /// <param name="loginModel">Thông tin tài khoản mật khẩu</param>
        /// <returns></returns>
        [HttpPost("Login")]
        public async Task<IActionResult> Login([FromBody] LoginModel loginModel)
        {
            try
            {

                if (ModelState.IsValid)// kiểm tra dữ liệu truyền vào
                {//nếu hợp lệ
                    var user =  _context.Users.Where(x => x.UserName == loginModel.UserName && x.Password == loginModel.PassWord).FirstOrDefault();
                    //;await _context.Users.FindAsync(loginModel.UserName);

                    if (user == null)
                    {
                        return StatusCode(StatusCodes.Status400BadRequest, "Login failed");
                    }
                    else
                    {
                       // var roles =  _context.UserRoles.Where(x => x.UserId == user.Id).ToList();
                        //var result = await _userService.Login(user,roles);
                        var result = await _userService.LoginSystem(loginModel);
                        
                        if (result.Success)//thành công
                        {
                            return StatusCode(StatusCodes.Status200OK, result.Data);
                        }
                        else//thất bại
                        {
                            return StatusCode(StatusCodes.Status401Unauthorized, result);
                        }
                    }
                    
                }
                else//dữ liệu truyền vào không hợp lệ
                {
                    return StatusCode(StatusCodes.Status400BadRequest,"Login failed");

                }
            }
            catch (Exception ex)
            {
                //trả về trạng thái lỗi cho client và mã lỗi cho hàm handle
                return StatusCode(StatusCodes.Status500InternalServerError,ex);

            }
        }
    }
}