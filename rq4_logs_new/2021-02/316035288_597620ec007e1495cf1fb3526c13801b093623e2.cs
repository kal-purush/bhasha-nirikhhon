using System;
using System.Collections.Generic;
using Chatbot.Model.DataModel;

namespace Chatbot.Core.Common
{
    public class DefaultSettingsSource
    {
        public static Settings[] GetDefaultSettings()
        {
            var list = new List<Settings>()
            {
                new Settings()
                {
                    Id = new Guid("00000000-0000-0000-0001-000000000001"),
                    Name = "beginShift",
                    Description = "Дата начала смены",
                    Value = "9"
                },
                new Settings()
                {
                    Id = new Guid("00000000-0000-0000-0001-000000000002"),
                    Name = "closeShift",
                    Description = "Дата окончания смены",
                    Value = "18"
                },
                new Settings()
                {
                    Id = new Guid("00000000-0000-0000-0001-000000000003"),
                    Name = "salam1",
                    Description = "Приветствие1",
                    Value = "Здравствуйте. Я чат бот АО РЭС, могу ответить на популярные вопросы."
                },
                new Settings()
                {
                    Id = new Guid("00000000-0000-0000-0001-000000000004"),
                    Name = "salam2",
                    Description = "Приветствие2",
                    Value =
                        "Здравствуйте! Я робот-помощник АО РЭС. Выберите тему из списка ниже или заполните форму для связи с оператором"
                },
                new Settings()
                {
                    Id = new Guid("00000000-0000-0000-0001-000000000005"),
                    Name = "sendedMessage",
                    Description = "Текст отправленного сообщения",
                    Value = "Ваше сообщение отправлено"
                },
                new Settings()
                {
                    Id = new Guid("00000000-0000-0000-0001-000000000006"),
                    Name = "caption",
                    Description = "Заголовок чатбота",
                    Value = "ЧАТ БОТ АО РЭС"
                }
            };

            return list.ToArray();
        }
    }
}