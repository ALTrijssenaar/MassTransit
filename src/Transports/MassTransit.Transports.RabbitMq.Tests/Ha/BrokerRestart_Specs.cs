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
    using RabbitMQ.Client.Events;
    using Util;


    [TestFixture]
    public class Restarting_the_broker
    {
        [Test]
        public void Should_continue_to_allow_publishing()
        {
            var retryPolicy = new FixedIntervalRetryPolicy(120, TimeSpan.FromSeconds(1));
            using (var connection = new HaConnection(_connectionFactory, retryPolicy, 1.Hours()))
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

        [Test]
        public void Should_resume_consumer_on_reconnect()
        {
            var retryPolicy = new FixedIntervalRetryPolicy(120, TimeSpan.FromSeconds(1));
            using (var connection = new HaConnection(_connectionFactory, retryPolicy, 1.Hours()))
            {
                using (IModel model = connection.CreateModel())
                {
                    try
                    {
                        model.ExchangeDeclare("haclient-test", ExchangeType.Fanout, true);
                        model.QueueDeclare("haclient-queue", false, true, true, null);
                        model.QueueBind("haclient-queue", "haclient-test", "");

                        var consumer = new QueueingBasicConsumer();

                        var consumerTag = model.BasicConsume("haclient-queue", false, consumer);

                        _serviceController.Restart();

                        model.BasicPublish("haclient-test", "", model.CreateBasicProperties(),
                            Encoding.UTF8.GetBytes("Hello, World."));

                        BasicDeliverEventArgs result;
                        var received = consumer.Queue.Dequeue(8000, out result);

                        Assert.IsTrue(received, "Consumer did not receive message");

                        var body = Encoding.UTF8.GetString(result.Body);
                        Assert.AreEqual("Hello, World.", body);

                        model.BasicCancel(consumerTag);
                    }
                    finally
                    {
                        model.QueueDelete("haclient-queue");
                        model.ExchangeDelete("haclient-test");
                    }
                }
            }
        }

        readonly RabbitMqServiceController _serviceController;
        readonly ConnectionFactory _connectionFactory;

        public Restarting_the_broker()
        {
            _serviceController = new RabbitMqServiceController();
            _connectionFactory = new ConnectionFactory
                {
                    HostName = "localhost",
                    VirtualHost = "/",
                };
        }
    }
}