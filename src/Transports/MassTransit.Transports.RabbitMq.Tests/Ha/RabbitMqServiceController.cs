// Copyright 2007-2012 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.
namespace MassTransit.Transports.RabbitMq.Tests.Ha
{
    using System;
    using System.Linq;
    using System.Security.Principal;
    using System.ServiceProcess;
    using Logging;


    public class RabbitMqServiceController
    {
        readonly ILog _log = Logger.Get<RabbitMqServiceController>();
        readonly string _serviceName;

        public RabbitMqServiceController()
        {
            _serviceName = "RabbitMQ";
        }

        public bool IsAdministrator
        {
            get
            {
                WindowsIdentity identity = WindowsIdentity.GetCurrent();

                if (null != identity)
                {
                    var principal = new WindowsPrincipal(identity);

                    return principal.IsInRole(WindowsBuiltInRole.Administrator);
                }

                return false;
            }
        }

        public bool IsServiceInstalled()
        {
            return IsServiceInstalled(_serviceName);
        }

        public bool IsServiceInstalled(string serviceName)
        {
            return ServiceController.GetServices()
                                    .Any(service => string.CompareOrdinal(service.ServiceName, serviceName) == 0);
        }

        public bool IsServiceStopped()
        {
            return IsServiceStopped(_serviceName);
        }

        public bool IsServiceStopped(string serviceName)
        {
            using (var sc = new ServiceController(serviceName))
            {
                return sc.Status == ServiceControllerStatus.Stopped;
            }
        }

        public void StartService()
        {
            StartService(_serviceName);
        }

        public void StartService(string serviceName)
        {
            using (var sc = new ServiceController(serviceName))
            {
                if (sc.Status == ServiceControllerStatus.Running)
                {
                    _log.InfoFormat("The {0} service is already running.", serviceName);
                    return;
                }

                if (sc.Status == ServiceControllerStatus.StartPending)
                {
                    _log.InfoFormat("The {0} service is already starting.", serviceName);
                    return;
                }

                if (sc.Status == ServiceControllerStatus.Stopped || sc.Status == ServiceControllerStatus.Paused)
                {
                    sc.Start();
                    sc.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromSeconds(10));
                }
                else
                {
                    // Status is StopPending, ContinuePending or PausedPending, print warning
                    _log.WarnFormat(
                        "The {0} service can't be started now as it has the status {1}. Try again later...", serviceName,
                        sc.Status.ToString());
                }
            }
        }

        public void StopService()
        {
            StopService(_serviceName);
        }

        public void StopService(string serviceName)
        {
            using (var sc = new ServiceController(serviceName))
            {
                if (sc.Status == ServiceControllerStatus.Stopped)
                {
                    _log.InfoFormat("The {0} service is not running.", serviceName);
                    return;
                }

                if (sc.Status == ServiceControllerStatus.StopPending)
                {
                    _log.InfoFormat("The {0} service is already stopping.", serviceName);
                    return;
                }

                if (sc.Status == ServiceControllerStatus.Running || sc.Status == ServiceControllerStatus.Paused)
                {
                    sc.Stop();
                    sc.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(10));
                }
                else
                {
                    // Status is StartPending, ContinuePending or PausedPending, print warning
                    _log.WarnFormat(
                        "The {0} service can't be stopped now as it has the status {1}. Try again later...", serviceName,
                        sc.Status.ToString());
                }
            }
        }

        public void Restart()
        {
            StopService();
            StartService();
        }
    }
}