// Copyright (c) Arctium Emulation.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Framework.Attributes;
using Framework.Cryptography;
using Framework.Database.Bnet;
using Framework.Logging;
using Framework.Misc;

namespace BnetServer.Console.Commands
{
    public class AccountCommands
    {
        [ConsoleCommand("CreateAccount", 2, "")]
        public static void CreateAccount(CommandArgs args)
        {
            var email = args.Read<string>();
            var password = args.Read<string>();

            if (email != null && password != null)
            {
                var salt = new byte[0].GenerateRandomKey(32).ToHexString();

                if (!Database.Bnet.Any<Account>(a => a.Email == email))
                {
                    var account = new Account
                    {
                        Email = email,
                        PasswordVerifier = PasswordHash.GeneratePasswordVerifier(email, password, salt).ToHexString(),
                        Salt = salt,
                        // Region = XX
                        Region = 7
                    };

                    var gameAccount = new GameAccount
                    {
                        AccountId = Database.Bnet.GetAutoIncrementValue<Account, uint>(),
                        Game = "WoW",
                        Index = 1,
                        // Region = XX
                        Region = 7,
                        ExpansionLevel = 6
                    };

                    if (Database.Bnet.Add(account) && Database.Bnet.Add(gameAccount))
                        Log.Message(LogTypes.Success, $"Account '{email}' successfully created.");
                    else
                        Log.Message(LogTypes.Error, $"Account creation failed.");
                }
                else
                    Log.Message(LogTypes.Error, $"Account '{email}' already in database.");
            }
        }

        [ConsoleCommand("DeleteAccount", 1, "")]
        public static void DeleteAccount(CommandArgs args)
        {
            var email = args.Read<string>();

            if (email != "")
            {
                var account = Database.Bnet.Single<Account>(a => a.Email == email);

                if (account != null)
                {
                    if (Database.Bnet.Delete(account))
                        Log.Message(LogTypes.Success, $"Account '{account.Email}' successfully deleted.");
                    else
                        Log.Message(LogTypes.Error, $"Can't delete account '{account.Email}'.");
                }
                else
                    Log.Message(LogTypes.Warning, $"Account '{email}' doesn't exist.");
            }
        }
    }
}