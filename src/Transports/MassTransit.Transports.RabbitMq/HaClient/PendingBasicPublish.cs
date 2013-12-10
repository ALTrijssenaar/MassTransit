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
    using System;
    using System.Threading;
    using RabbitMQ.Client;
    using Util;


    public class PendingBasicPublish
    {
        readonly ulong _next;
        readonly string _exchange;
        readonly string _routingKey;
        readonly bool _mandatory;
        readonly bool _immediate;
        readonly IBasicProperties _basicProperties;
        readonly byte[] _body;
        readonly object _monitor = new object();
        bool _ackd;

        public PendingBasicPublish(ulong next, string exchange, string routingKey, bool mandatory, bool immediate,
            IBasicProperties basicProperties, byte[] body)
        {
            _next = next;
            _exchange = exchange;
            _routingKey = routingKey;
            _mandatory = mandatory;
            _immediate = immediate;
            _basicProperties = basicProperties;
            _body = body;
        }

        public void Ack()
        {
            using (TimedLock.Lock(_monitor, 30000))
            {
                _ackd = true;
                Monitor.Pulse(_monitor);
            }
        }

        public void Wait(int timeout)
        {
            using (TimedLock.Lock(_monitor, timeout))
            {
                while (false == _ackd)
                {
                    if (!Monitor.Wait(_monitor, timeout))
                        throw new LockTimeoutException();
                }
            }
        }
    }
}