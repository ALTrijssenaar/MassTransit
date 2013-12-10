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
    using System.Collections.Generic;
    using Logging;
    using Util;


    public class HaBasicConsumerList :
        IBasicConsumerEventSink
    {
        static readonly ILog _log = Logger.Get<HaBasicConsumerList>();

        readonly IDictionary<DeliveryKey, PendingDelivery> _deliveries;
        readonly IHaModel _model;
        readonly object _monitor = new object();
        readonly IDictionary<string, IHaBasicConsumer> _consumers;
        readonly int _lockTimeout;

        public HaBasicConsumerList(IHaModel model, int lockTimeout)
        {
            _model = model;
            _lockTimeout = lockTimeout;

            _consumers = new Dictionary<string, IHaBasicConsumer>();
            _deliveries = new Dictionary<DeliveryKey, PendingDelivery>();
        }

        public void DeliveryReceived(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey)
        {
            var key = new DeliveryKey(consumerTag, deliveryTag);

            _deliveries[key] = new PendingDelivery(consumerTag, deliveryTag, redelivered, exchange, routingKey);
        }

        public void DeliveryCompleted(string consumerTag, ulong deliveryTag)
        {
            var key = new DeliveryKey(consumerTag, deliveryTag);

            _deliveries.Remove(key);
        }

        public void DeliveryFaulted(string consumerTag, ulong deliveryTag, Exception exception)
        {
            var key = new DeliveryKey(consumerTag, deliveryTag);

            PendingDelivery delivery;
            if (_deliveries.TryGetValue(key, out delivery))
            {
                // ultimately want to redirect the deliveryTag to the same consumer for processing
                if (_log.IsDebugEnabled)
                {
                    _log.DebugFormat("Delivery {0} faulted for consumer {1} from exchange {2} on connection {3}",
                        consumerTag, deliveryTag, delivery.Exchange, _model.Connection);
                }


                _deliveries.Remove(key);
            }
        }

        public void RegisterConsumer(string consumerTag, IHaBasicConsumer haBasicConsumer)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                _consumers[consumerTag] = haBasicConsumer;
            }
        }

        public void CancelConsumer(string consumerTag)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                _consumers.Remove(consumerTag);
            }
        }

        public void AddConsumer(string consumerTag, IHaBasicConsumer consumer)
        {
            if (_consumers.ContainsKey(consumerTag))
            {
                if (_log.IsWarnEnabled)
                    _log.WarnFormat("Duplicate consumer tag {0} added on connection {1}", consumerTag, _model.Connection);
            }

            _consumers[consumerTag] = consumer;
        }


        public bool TryGetConsumer(string consumerTag, out IHaBasicConsumer consumer)
        {
            return _consumers.TryGetValue(consumerTag, out consumer);
        }


        public void Remove(string consumerTag)
        {
            _consumers.Remove(consumerTag);
        }


        struct DeliveryKey :
            IEquatable<DeliveryKey>
        {
            public readonly string ConsumerTag;
            public readonly ulong DeliveryTag;

            public DeliveryKey(string consumerTag, ulong deliveryTag)
            {
                ConsumerTag = consumerTag;
                DeliveryTag = deliveryTag;
            }

            public bool Equals(DeliveryKey other)
            {
                return string.Equals(ConsumerTag, other.ConsumerTag) && DeliveryTag == other.DeliveryTag;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                return obj is DeliveryKey && Equals((DeliveryKey)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (ConsumerTag.GetHashCode() * 397) ^ (int)DeliveryTag;
                }
            }
        }
    }
}