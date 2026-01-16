using Microsoft.AspNetCore.Http;
using Neo.IO.Json;
using Neo.Wallets.NEP6;
using System;

namespace Neo.Plugins
{
    public class LockWallet : Plugin, IRpcPlugin
    {
        private NEP6Wallet wallet = null;
        private IDisposable locker = null;
        protected override bool OnMessage(object message)
        {
            if (message is object[] args && args[0] is NEP6Wallet wallet)
            {
                return OnInit(wallet, args[1] as string);
            }
            return false;
        }
        public JObject OnProcess(HttpContext context, string method, JArray _params)
        {
            Console.WriteLine(method);
            switch (method)
            {
                case "lockwallet":
                    return OnLock();
                case "unlockwallet":
                    return OnUnLock(_params);
                default:
                    return null;
            }
        }
        private bool OnInit(NEP6Wallet wallet, string password)
        {
            this.wallet = wallet;
            this.locker = wallet.Unlock(password);
            return true;
        }
        private JObject OnLock()
        {
            if (locker == null)
                return false;
            this.locker.Dispose();
            return true;
        }

        private JObject OnUnLock(JArray _params)
        {
            if (wallet == null)
                return false;
            this.locker = wallet.Unlock(_params[0].AsString());
            return true;
        }
    }
}