using System;
using LT.DigitalOffice.Models.Broker.Enums;
using LT.DigitalOffice.TextTemplateService.Models.Db;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace LT.DigitalOffice.TextTemplateService.Data.Provider.MsSql.Ef.Migrations
{
  [Migration("20220303141200_AddTemplateTexts")]
  [DbContext(typeof(TextTemplateServiceDbContext))]
  public class AddTemplateTexts : Migration
  {
    protected override void Up(MigrationBuilder migrationBuilder)
    {
      Guid PasswordRecoveryTemplate = Guid.NewGuid();
      Guid GreetingTemplate = Guid.NewGuid();
      Guid MailConfirmationTemplate = Guid.NewGuid();
      Guid UserRecoveryTemplate = Guid.NewGuid();

      const string ru = "ru";
      const string en = "en";

      migrationBuilder.InsertData(
        table: DbTemplate.TableName,
        columns: new[]
        {
          nameof(DbTemplate.Id),
          nameof(DbTemplate.Type),
          nameof(DbTemplate.IsActive)
        },
        columnTypes: new string[]
        {
          "uniqueidentifier",
          "int",
          "bit"
        },
        values: new object[,]
        {
          { PasswordRecoveryTemplate, (int)TemplateType.PasswordRecovery, true},
          { GreetingTemplate, (int)TemplateType.Greeting, true},
          { MailConfirmationTemplate, (int)TemplateType.ConfirmСommunication, true},
          { UserRecoveryTemplate, (int)TemplateType.Notification, true}
        });

      migrationBuilder.InsertData(
        table: DbTextTemplate.TableName,
        columns: new[]
        {
          nameof(DbTextTemplate.Id),
          nameof(DbTextTemplate.TemplateId),
          nameof(DbTextTemplate.Name),
          nameof(DbTextTemplate.Subject),
          nameof(DbTextTemplate.Text),
          nameof(DbTextTemplate.Locale),
          nameof(DbTextTemplate.IsActive)
        },
        columnTypes: new string[]
        {
          "uniqueidentifier",
          "uniqueidentifier",
          "nvarchar(MAX)",
          "nvarchar(MAX)",
          "nvarchar(MAX)",
          "nvarchar(2)",
          "bit"
        },
        values: new object[,]
        {
          {
            Guid.NewGuid(),
            PasswordRecoveryTemplate,
            "Password recovery",
            "Password recovery",
            "Hello {[FirstName]} {[LastName]}!\n"
            + "This is an email for resetting the password to your account. If you did not leave a request, please ignore this message.\n"
            + "Follow this link: https://dev.ltdo.xyz/auth/reset?userId={[Id]}\n"
            + "You will need to enter this code to reset your password: {[Password]}",
            en,
            true
          },
          {
            Guid.NewGuid(),
            PasswordRecoveryTemplate,
            "Восстановление пароля",
            "Восстановление пароля",
            "Здравствуйте, {[FirstName]} {[LastName]}!\n"
            + "Это письмо для смены пароля к учетной записи в системе. Если вы не оставляли заявку, проигнорируйте это сообщение.\n"
            + "Перейдите по этой ссылке: https://dev.ltdo.xyz/auth/reset?userId={[Id]}\n"
            + "Вам будет необходимо ввести код для смены пароля: {[Password]}",
            ru,
            true
          },
          {
            Guid.NewGuid(),
            GreetingTemplate,
            "Greeting",
            "Greeting",
            "Hello {[FirstName]} {[LastName]}!\n"
            + "This is an email for registration. If you did not leave a request, please ignore this message.\n"
            + "Follow this link: https://dev.ltdo.xyz/auth/firstlogin?userId={[Id]}\nLogin: You will need to come up with a new login for your profile. "
            + "You can use it in the future to log in to the system. Password: {[Password]}",
            en,
            true
          },
          {
            Guid.NewGuid(),
            GreetingTemplate,
            "Приглашение",
            "Приглашение",
            "Здравствуйте, {[FirstName]} {[LastName]}!\n"
            + "Это письмо для прохождения регистрации. Если вы не оставляли заявку, проигнорируйте это сообщение.\n"
            + "Перейдите по этой ссылке: https://dev.ltdo.xyz/auth/firstlogin?userId={[Id]}\nЛогин: Вам будет необходимо придумать и ввести новый логин. "
            + "Он в будущем понадобится вам для входа. Пароль: {[Password]}",
            ru,
            true
          },
          {
            Guid.NewGuid(),
            MailConfirmationTemplate,
            "Mail confirmation",
            "Mail confirmation",
            "Hello {[FirstName]} {[LastName]}!\n"
            + "This is an email for confirming adding a new email to your profile. If you did not leave a request, please ignore this message.\n"
            + "Follow this link: \nThis link will be active for 30 minutes",
            en,
            true
          },
          {
            Guid.NewGuid(),
            MailConfirmationTemplate,
            "Подтверждение электронной почты",
            "Подтверждение электронной почты",
            "Здравствуйте, {[FirstName]} {[LastName]}!\n"
            + "Это письмо для подтверждения добавления в профиль новой электронной почты. Если заявка не от тебя, проигнорируй это сообщение.\n"
            + "Перейди по этой ссылке: \nСсылка будет активна в течение 30 минут.",
            ru,
            true
          },
          {
            Guid.NewGuid(),
            UserRecoveryTemplate,
            "User recovery",
            "User recovery",
            "Hello {[FirstName]} {[LastName]}!\n"
            + "This is an email for registration. If you did not leave a request, please ignore this message.\n"
            + "Follow this link: https://dev.ltdo.xyz/auth/firstlogin?userId={[Id]}\nPassword: {[Password]}",
            en,
            true
          },
          {
            Guid.NewGuid(),
            UserRecoveryTemplate,
            "Восстановление пользователя",
            "Восстановление пользователя",
            "Здравствуйте, {[FirstName]} {[LastName]}!\n"
            + "Это письмо для прохождения регистрации. Если вы не оставляли заявку, проигнорируйте это сообщение.\n"
            + "Перейдите по этой ссылке: https://dev.ltdo.xyz/auth/firstlogin?userId={[Id]}\nПароль: {[Password]}",
            ru,
            true
          }
        });
    }
  }
}