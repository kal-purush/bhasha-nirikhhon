/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using TheXDS.MCART.Attributes;
using TheXDS.MCART.Component;
using TheXDS.MCART.PluginSupport.Legacy;
using TheXDS.MCART.Types.Extensions;
using TheXDS.Proteus.Misc;
using TheXDS.Proteus.Plugins;
using static TheXDS.MCART.Objects;

namespace TheXDS.Proteus.Tools
{
    public class ExDumper : Tool
    {
        private const string _totalFail = "Error cerrando la aplicación. Para prevenir daños a la información o al equipo, detenga la ejecución de este programa inmediatamente.";
        private static bool _dumping;
        private static bool _enabled;
        private static bool _shutdown = true;

        /// <summary>
        ///     Inicializa la clase <see cref="ExDumper"/>
        /// </summary>
        static ExDumper()
        {
            Hook();

            var i = new AssemblyInfo();
            Proteus.AlertTarget?.Alert($"{i.Name} se ha inicializado", $@"{i.Name} {i.InformationalVersion}

Captura de excepciones {(_enabled ? "activada" : "desactivada")}
Cierre forzoso {(_shutdown ? "activado" : "desactivado")}");
        }

        [InteractionItem, Name("💣"), Description("Genera una excepción de manera intencional.")]
        public void IntentionallyCrash(object sender, EventArgs e)
        {
            /* NOTA:
             * Este bloque genera intencionalmente una excepción de forma 
             * aleatoria. Obviamente, si el depurador se detiene aquí, se debe
             * hacer caso omiso, y simplemente dejar que la ejecución continúe
             * normalmente para que este plugin siga con su propósito.
             */
            throw GetTypes<Exception>(true).Pick().New<Exception>();
        }

        [InteractionItem, Name("💣😉"), Description("Simula una excepción en el sistema y muestra una ventana de error crítico.")]
        public void IntentionallyPick(object sender, EventArgs e)
        {
            Exception ex;
            try
            {
                ex = GetTypes<Exception>(true).Pick().New<Exception>();
            }
            catch (Exception ee)
            {
                ex = ee;
            }
            Proteus.MessageTarget?.Critical(ex);
        }

        [InteractionItem, Name("🕷"), Description("Activa/desactiva la captura de excepciones por este plugin.")]
        public void ToggleCatch(object sender, EventArgs e)
        {
            if (_enabled) UnHook();
            else Hook();
            var m = $"Captura de excepciones {(_enabled ? "activada" : "desactivada")}.";
            Proteus.MessageTarget?.Info(m);
            Proteus.AlertTarget?.Alert(m, _enabled
                ? "Las excepciones no controladas serán capturadas y volcadas a un archivo de texto en el escritorio."
                : "Las excepciones no controladas no serán capturadas.");
        }

        [InteractionItem, Name("🕷→❌"), Description("Activa/desactiva el cierre forzoso de la aplicación al producirse un error crítico.")]
        public void ToggleShutdown(object sender, EventArgs e)
        {
            _shutdown = !_shutdown;
            var m = $"Cierre forzoso {(_shutdown ? "activado" : "desactivado")}.";
            Proteus.MessageTarget?.Info(m);
            Proteus.AlertTarget?.Alert(m, _shutdown
                ? "Las excepciones críticas causarán el cierre de la aplicación."
                : "Las excepciones críticas no causarán el cierre de la aplicación.");
        }

        private static void Hook()
        {
            if (_enabled) return;
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            Application.Current.Dispatcher.UnhandledException += Dispatcher_UnhandledException;
            Application.Current.DispatcherUnhandledException += Dispatcher_UnhandledException;
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;
            _enabled = true;
        }
        private static void UnHook()
        {
            if (!_enabled) return;
            AppDomain.CurrentDomain.UnhandledException -= CurrentDomain_UnhandledException;
            Application.Current.Dispatcher.UnhandledException -= Dispatcher_UnhandledException;
            Application.Current.DispatcherUnhandledException -= Dispatcher_UnhandledException;
            TaskScheduler.UnobservedTaskException -= TaskScheduler_UnobservedTaskException;
            _enabled = false;
        }
        private static void Fail(Exception? e)
        {
            if (e is null)
            {
                Environment.FailFast(_totalFail);
                return;
            }
            try
            {
                Proteus.MessageTarget?.Critical(e);
                Dump(e);
                if (_shutdown || !Debugger.Launch())
                {
                    Application.Current.Dispatcher.Invoke(() => Application.Current?.Shutdown(e.HResult));
                }
                else
                {
                    Debugger.Break();
                }
            }
            catch (Exception ex)
            {
                try
                {
                    App.UiInvoke(() => Proteus.MessageTarget?.Critical(_totalFail));
                }
                catch { }
                Environment.FailFast(_totalFail, ex);
            }
        }
        private static void TaskScheduler_UnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            e.SetObserved();
            Fail(e.Exception);
        }
        private static void Dispatcher_UnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
        {
            e.Handled = true;
            Fail(e.Exception);
        }
        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Fail(e.ExceptionObject as Exception);
        }
        private static void Dump(Exception ex)
        {
            if (_dumping) return;
            _dumping = true;
            try
            {
                var failLog = $"{Environment.GetFolderPath(Environment.SpecialFolder.Desktop)}\\Proteus-{DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss")}-failure.txt";
                using var j = new System.IO.StreamWriter(failLog);
                Internal.Dump(j, ex);
            }
            catch { /* Everything failed. */ }
            _dumping = false;
        }
    }
}