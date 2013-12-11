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
    using System.Threading;
    using RabbitMQ.Client;
    using Util;


    public class PendingBasicPublish
    {
        readonly IBasicProperties _basicProperties;
        readonly byte[] _body;
        readonly string _exchange;
        readonly bool _immediate;
        readonly bool _mandatory;
        readonly object _monitor = new object();
        readonly string _routingKey;
        readonly ulong _sequenceNumber;
        bool _ackd;
        ulong _modelSequenceNumber;

        public PendingBasicPublish(ulong sequenceNumber, ulong modelSequenceNumber, string exchange, string routingKey,
            bool mandatory, bool immediate, IBasicProperties basicProperties, byte[] body)
        {
            _sequenceNumber = sequenceNumber;
            _modelSequenceNumber = modelSequenceNumber;
            _exchange = exchange;
            _routingKey = routingKey;
            _mandatory = mandatory;
            _immediate = immediate;
            _basicProperties = basicProperties;
            _body = body;
        }

        public ulong SequenceNumber
        {
            get { return _sequenceNumber; }
        }

        public ulong ModelSequenceNumber
        {
            get { return _modelSequenceNumber; }
        }

        public string Exchange
        {
            get { return _exchange; }
        }

        public string RoutingKey
        {
            get { return _routingKey; }
        }

        public bool Mandatory
        {
            get { return _mandatory; }
        }

        public bool Immediate
        {
            get { return _immediate; }
        }

        public IBasicProperties BasicProperties
        {
            get { return _basicProperties; }
        }

        public byte[] Body
        {
            get { return _body; }
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

        public void Nack()
        {
        }

        public ulong Republish(IModel model, int timeout)
        {
            using (TimedLock.Lock(_monitor, timeout))
            {
                ulong modelSequenceNumber = model.NextPublishSeqNo;
                model.BasicPublish(_exchange, _routingKey, _mandatory, _immediate, _basicProperties, _body);

                _modelSequenceNumber = modelSequenceNumber;

                return modelSequenceNumber;
            }
        }
    }
}