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
    using System.Text;
    using HaClient;
    using Magnum.Extensions;
    using NUnit.Framework;
    using RabbitMQ.Client;
    using Util;


    [TestFixture]
    public class Restarting_the_broker
    {
        [Test]
        public void Should_continue_to_allow_publishing()
        {
            var connectionFactory = new ConnectionFactory
                {
                    HostName = "localhost",
                    VirtualHost = "/",
                };

            var retryPolicy = new FixedIntervalRetryPolicy(120, TimeSpan.FromSeconds(1));
            using (var connection = new HaConnection(connectionFactory, retryPolicy, 1.Hours()))
            {
                using (IModel model = connection.CreateModel())
                {
                    try
                    {
                        model.ExchangeDeclare("haclient-test", ExchangeType.Fanout, true);

                        _serviceController.Restart();

                        model.BasicPublish("haclient-test", "", model.CreateBasicProperties(),
                            Encoding.UTF8.GetBytes("Hello, World."));
                    }
                    finally
                    {
                        model.ExchangeDelete("haclient-test");
                    }
                }
            }
        }

        readonly RabbitMqServiceController _serviceController;

        public Restarting_the_broker()
        {
            _serviceController = new RabbitMqServiceController();
        }
    }
}