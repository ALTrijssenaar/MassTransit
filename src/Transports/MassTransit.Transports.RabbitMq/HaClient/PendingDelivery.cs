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
    public class PendingDelivery
    {
        readonly string _consumerTag;
        readonly ulong _deliveryTag;
        readonly string _exchange;
        readonly bool _redelivered;
        readonly string _routingKey;

        public PendingDelivery(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey)
        {
            _consumerTag = consumerTag;
            _deliveryTag = deliveryTag;
            _redelivered = redelivered;
            _exchange = exchange;
            _routingKey = routingKey;
        }

        public string Exchange
        {
            get { return _exchange; }
        }
    }
}