using AuthProvider;
using Microsoft.EntityFrameworkCore;
using Models;
using SqlContext;
using System;
using System.Collections.Generic;
using System.IO;

namespace DMG.Services
{
    public interface IHangFireService
    {
        void DeleteDatabase();
        void InsertNewData();
    }

    public class HangFireService : IHangFireService
    {
        private DataContext _ctx;

        public HangFireService(DataContext ctx)
        {
            _ctx = ctx;
        }

        public void DeleteDatabase()
        {
            try
            {
                _ctx.Database.ExecuteSqlCommand("Delete From [qualco4].[dbo].[Bill]");
                _ctx.Database.ExecuteSqlCommand("Delete From [qualco4].[dbo].[Settlement]");
                _ctx.SaveChangesAsync();
            }
            catch (Exception ex)
            {

            }

        }

        public void InsertNewData()
        {
            var users = new List<User>();
            var bills = new List<Bill>();
            var uniqueUsers = new List<string>();
            string[] headers;
            string[] fields;
            int counter = 0;
            var fileStream = File.ReadAllLines(@"D:\git\DebtManagment\assets\CitizenDebts_1M_3.txt");
            var psw = PasswordHasher.HashPassword("123");
            var firstline = fileStream[0];
            headers = firstline.Split(";");
            try
            {
                for (var j = 1; j < fileStream.Length; j++)
                {
                    User user = new User();
                    Bill debt = new Bill();
                    user.Password = psw;
                    fields = fileStream[j].Split(";");
                    user.Name = fields[1];
                    user.Lastname = fields[2];
                    user.Address.Street = fields[5];
                    user.Vat = fields[0];
                    user.Address.County = fields[6];
                    user.Email = fields[3];
                    user.Mobile = fields[4];
                    user.FirstLogin = true;
                    user.LastUpdate = DateTime.Now;
                    debt.Amount = Double.Parse(fields[9]);
                    debt.Description = fields[8];
                    debt.DateDue = ConvertDate(fields[10]);
                    debt.Bill_Id = fields[7];
                    debt.LastUpdate = DateTime.Now;

                    var index = uniqueUsers.FindIndex(i => i == user.Vat);
                    if (index > -1)
                    {
                        users[index].Bills.Add(debt);
                    }
                    else
                    {
                        uniqueUsers.Add(user.Vat);
                        user.Bills.Add(debt);
                        users.Add(user);
                    }
                }
                _ctx.Set<User>().AddRange(users);
                _ctx.SaveChanges();
                _ctx.Dispose();
            }
            catch (Exception ex)
            {
                var error = counter;
                var _ex = ex;
            }
        }

        private static DateTime ConvertDate(string date)
        {
            var dateformat = date.Substring(0, 4) + "-" + date.Substring(4, 2) + "-" + date.Substring(6, 2);
            return DateTime.Parse(dateformat);
        }
    }
}