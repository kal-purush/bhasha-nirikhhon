using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;

namespace FinLit.Controllers
{
    public class BaseController : Controller
    {
        /// <summary>
        /// Получает ID текущего пользователя из сессии.
        /// Если пользователь не авторизован — перенаправляет на страницу авторизации.
        /// </summary>
        /// <returns>int — ID пользователя</returns>
        protected int GetUserIdOrRedirect()
        {
            var userId = HttpContext.Session.GetInt32("UserId");

            if (userId == null)
            {
                // Прерываем выполнение и делаем редирект вручную
                Response.Redirect(Url.Action("AuthentificationView", "User") ?? "/");
                return 0; // 0 — сигнал, что редирект уже произошёл
            }

            return userId.Value;
        }

        //protected int GetSelectedAccountId() // может переписать через сервис нужно подумать или передавать в этот метод объект где используется или в конструктор Базового класса но тогда в base(персонализация)
        //{
        //    var sessionAccountId = HttpContext.Session.GetInt32("SelectedAccountId");
        //    if (sessionAccountId.HasValue)
        //        return sessionAccountId.Value;

        //    // Если в сессии нет — взять из персонализации
        //    var personalization = GetPersonalization();
        //    return personalization?.AccountId ?? 0; // или выбросить исключение, если не задан
        //}

    }
}