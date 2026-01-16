//Copyright 2018 Damir Garipov
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

using System;
using NLog;

namespace DataBusService
{
    internal class Log
    {
        private static ILogger _logger;

        private static ILogger Logger => _logger ?? (_logger = LogManager.GetLogger("DataBusService"));

        public static void Info(string message)
        {
            Logger.Info(message);
        }

        public static void Debug(string message)
        {
            Logger.Debug(message);
        }

        public static void Warn(string message)
        {
            Logger.Warn(message);
        }

        public static void Error(string message, Exception exception)
        {
            Logger.Error(exception, message);
        }

        public static void Error(Exception exception)
        {
            Logger.Error(exception);
        }

        public static void Fatal(Exception exception)
        {
            Fatal(null, exception);
        }

        public static void Fatal(string message, Exception exception)
        {
            Logger.Fatal(exception, message);
        }
    }
}