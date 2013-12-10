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
namespace MassTransit.Transports.RabbitMq.HaClient
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;


    public class HaBasicConsumer :
        IBasicConsumer
    {
        readonly IBasicConsumer _consumer;

        public HaBasicConsumer(IBasicConsumer consumer)
        {
            _consumer = consumer;
        }

        public void HandleBasicConsumeOk(string consumerTag)
        {
            _consumer.HandleBasicConsumeOk(consumerTag);
        }

        public void HandleBasicCancelOk(string consumerTag)
        {
            _consumer.HandleBasicCancelOk(consumerTag);
        }

        public void HandleBasicCancel(string consumerTag)
        {
            _consumer.HandleBasicCancel(consumerTag);
        }

        public void HandleModelShutdown(IModel model, ShutdownEventArgs reason)
        {
            _consumer.HandleModelShutdown(model, reason);
        }

        public void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey, IBasicProperties properties, byte[] body)
        {
            _consumer.HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);
        }

        public IModel Model
        {
            get { return _consumer.Model; }
        }

        public event ConsumerCancelledEventHandler ConsumerCancelled
        {
            add { _consumer.ConsumerCancelled += value; }
            remove { _consumer.ConsumerCancelled -= value; }
        }
    }
}