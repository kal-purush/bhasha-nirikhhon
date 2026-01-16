using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using Wonga.ServiceTesting.EndpointLauncher.Validators;

namespace Wonga.ServiceTesting.EndpointLauncher.Tests
{
    [TestFixture]
    public class FluentServiceLauncherTests
    {
        private const string TimeoutPath = @"c:\windows\system32\timeout.exe";

        [Test]
        public void FluentServiceLauncher_should_start_multiple_processes()
        {
            var endpoints = new FluentServiceLauncher()
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 3"), new ProcessWaitHealthValidator(TimeSpan.FromMilliseconds(200)))
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 3"), new ProcessWaitHealthValidator(TimeSpan.FromMilliseconds(200)))
                .LaunchAll();
            Assert.DoesNotThrow(() =>
            {
                foreach (var e in endpoints)
                    e.ValidateHealth();
            });
        }

        [Test]
        public void FluentServiceLauncher_should_throw_if_application_finish_before_timeout()
        {
            var ex = Assert.Throws<AggregateException>(
                () => new FluentServiceLauncher()
                    .AddEndpoint(
                        l => l.LaunchApplication(TimeoutPath, "/T 1"),
                        new ProcessWaitHealthValidator(TimeSpan.FromSeconds(2)))
                    .LaunchAll());
            Assert.That(ex.InnerException.Message, Is.EqualTo(string.Format("Process '{0} /T 1' stopped unexpectedly with error code: 0", TimeoutPath)));
        }

        [Test]
        public void FluentServiceLauncher_should_start_endpoints_with_delay()
        {
            var delay = TimeSpan.FromSeconds(1);
            var endpoints = new FluentServiceLauncher()
                .WithLaunchDelay(delay)
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 3"), new ProcessWaitHealthValidator(TimeSpan.FromMilliseconds(200)))
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 3"), new ProcessWaitHealthValidator(TimeSpan.FromMilliseconds(200)))
                .LaunchAll();

            Assert.That(endpoints[1].Process.StartTime - endpoints[0].Process.StartTime, Is.EqualTo(delay).Within(TimeSpan.FromMilliseconds(200)));
        }

        [Test]
        public void FluentServiceLauncher_should_kill_started_endpoints_if_any_endpoint_fail_to_start()
        {
            var total = 10;
            var processes = new List<Process>();

            var launcher = new FluentServiceLauncher();

            for (var i = 0; i < total; ++i)
            {
                launcher.AddEndpoint(l =>
                {
                    var process = l.LaunchApplication(TimeoutPath, "/T 3");
                    processes.Add(process);
                    return process;
                });
            }

            launcher.AddEndpoint(l => { throw new Exception("abc"); });

            Assert.Throws<Exception>(() => launcher.LaunchAll());

            Assert.That(processes.Count, Is.EqualTo(total));
            Assert.That(processes.All(p => p.HasExited), Is.True);
        }

        [Test]
        public void FluentServiceLauncher_should_stop_processes_if_requested()
        {
            var endpoints = new FluentServiceLauncher()
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 3"), new ProcessWaitHealthValidator(TimeSpan.FromMilliseconds(200)))
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 3"), new ProcessWaitHealthValidator(TimeSpan.FromMilliseconds(200)))
                .LaunchAll();

            FluentServiceLauncher.TerminateEndpoints(endpoints);

            Assert.That(endpoints.All(e => e.Process.HasExited), Is.True);
        }

        [Test]
        public void FluentServiceLauncher_should_allow_retrying_endpoint_restart()
        {
            int n1 = 0;
            int n2 = 0;
            var launcher = new FluentServiceLauncher()
                .WithRetries(1)
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T " + (5 * (n1++)).ToString()), new ProcessWaitHealthValidator(TimeSpan.FromSeconds(1)))
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T " + (5 * (n2++)).ToString()), new ProcessWaitHealthValidator(TimeSpan.FromSeconds(1)));

            Assert.DoesNotThrow(() => launcher.LaunchAll());
        }

        [Test]
        public void FluentServiceLauncher_should_throw_after_retrying_fail()
        {
            var launcher = new FluentServiceLauncher()
                .WithRetries(3)
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 0"), new ProcessWaitHealthValidator(TimeSpan.FromSeconds(1)))
                .AddEndpoint(l => l.LaunchApplication(TimeoutPath, "/T 5"), new ProcessWaitHealthValidator(TimeSpan.FromSeconds(1)));

            var ex = Assert.Throws<AggregateException>(() => launcher.LaunchAll());

            Assert.That(ex.InnerException.Message, Is.EqualTo(string.Format("Process '{0} /T 0' stopped unexpectedly with error code: 0", TimeoutPath)));
        }
    }
}