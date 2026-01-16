using Microsoft.AspNetCore.Mvc;

#region PersonController，使用查询字符串指定版本

namespace BookLib.Controllers.V1
{
    [ApiVersion("1.0")]
    [Route("api/person")]
    [ApiController] //要用version区分不同的controller，必须要加[ApiController]
    public class PersonController : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> Get() => "Result fron v1";
    }
}

namespace BookLib.Controllers.V2
{
    [ApiVersion("2.0")]
    [Route("api/person")]
    [ApiController]
    public class PersonController : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> Get() => "Result fron v2";
    }
}

#endregion PersonController，使用查询字符串指定版本

#region StudentController，使用URL路径指定版本

namespace BookLib.API.Controllers.V1
{
    [ApiVersion("1.0")]
    [Route("api/v{version:apiVersion}/students")]//根据api的url判断api版本
    [ApiController]
    public class StudentController : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> Get() => "Result from v1";
    }
}

namespace BookLib.API.Controllers.V2
{
    [ApiVersion("2.0")]
    [Route("api/v{version:apiVersion}/students")]
    [ApiController]
    public class StudentController : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> Get() => "Result from v2";
    }
}

#endregion StudentController，使用URL路径指定版本

#region NewsController,MapToApiVersion示例

namespace BookLib.API.Controllers
{
    [Route("api/news")]
    [ApiVersion("1.0")]
    [ApiVersion("2.0")]
    [ApiController]
    public class NewsController : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> Get() => "Result from v1";

        //使用MapToApiVersion("2.0")可以将请求version2.0时map到该方法上
        [HttpGet, MapToApiVersion("2.0")]
        public ActionResult<string> GetV2() => "Result from v2";
    }
}

#endregion NewsController,MapToApiVersion示例

#region HelloWorldController，Deprecated示例

namespace BookLib.API.Controllers
{
    [ApiVersion("2.0")]
    [ApiVersion("1.0", Deprecated = true)]//Deprecated = true会在响应的header里面
    [Route("api/[controller]")]
    [ApiController]
    [NonController] //加了NonController就不再是一个有效api了
    public class HelloWorldController : Controller
    {
        [HttpGet]
        public string Get() => "Hello world!";

        [HttpGet, MapToApiVersion("2.0")]
        public string GetV2() => "Hello world v2.0!";
    }
}

#endregion HelloWorldController，Deprecated示例

#region ProjectController，使用Convention

namespace BookLib.API.Controllers.V1
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProjectController : Controller
    {
        [HttpGet]
        public string Get() => "Result from v1";
    }
}

namespace BookLib.API.Controllers.V2
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProjectController : Controller
    {
        [HttpGet]
        public string Get() => "Result from v2";
    }
}

#endregion ProjectController，使用Convention