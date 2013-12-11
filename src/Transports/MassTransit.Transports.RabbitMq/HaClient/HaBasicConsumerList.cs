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
    using System.Linq;
    using Logging;
    using Magnum.Extensions;
    using RabbitMQ.Client;
    using Util;


    public class HaBasicConsumerList :
        IBasicConsumerEventSink
    {
        static readonly ILog _log = Logger.Get<HaBasicConsumerList>();
        readonly IDictionary<string, IHaBasicConsumer> _consumers;

        readonly IDictionary<DeliveryKey, PendingDelivery> _deliveries;
        readonly int _lockTimeout;
        readonly IHaModel _model;
        readonly object _monitor = new object();

        public HaBasicConsumerList(IHaModel model, int lockTimeout)
        {
            _model = model;
            _lockTimeout = lockTimeout;

            _consumers = new Dictionary<string, IHaBasicConsumer>();
            _deliveries = new Dictionary<DeliveryKey, PendingDelivery>();
        }

        public int Count
        {
            get
            {
                using (TimedLock.Lock(_monitor, _lockTimeout))
                {
                    return _consumers.Count;
                }
            }
        }

        void IBasicConsumerEventSink.DeliveryReceived(string consumerTag, ulong deliveryTag, bool redelivered,
            string exchange,
            string routingKey)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                var key = new DeliveryKey(consumerTag, deliveryTag);

                _deliveries[key] = new PendingDelivery(consumerTag, deliveryTag, redelivered, exchange, routingKey);
            }
        }

        void IBasicConsumerEventSink.DeliveryCompleted(string consumerTag, ulong deliveryTag)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                var key = new DeliveryKey(consumerTag, deliveryTag);

                _deliveries.Remove(key);
            }
        }

        void IBasicConsumerEventSink.DeliveryFaulted(string consumerTag, ulong deliveryTag, Exception exception)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                var key = new DeliveryKey(consumerTag, deliveryTag);

                PendingDelivery delivery;
                if (_deliveries.TryGetValue(key, out delivery))
                {
                    // ultimately want to redirect the deliveryTag to the same consumer for processing
                    if (_log.IsDebugEnabled)
                    {
                        _log.DebugFormat(
                            "Delivery {0} faulted for consumer {1} from exchange {2} on connection {3}",
                            consumerTag, deliveryTag, delivery.Exchange, _model.Connection);
                    }


                    _deliveries.Remove(key);
                }
            }
        }

        void IBasicConsumerEventSink.RegisterConsumer(string consumerTag, IHaBasicConsumer haBasicConsumer)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                _consumers[consumerTag] = haBasicConsumer;
            }
        }

        void IBasicConsumerEventSink.CancelConsumer(string consumerTag)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                _consumers.Remove(consumerTag);
            }
        }

        public bool TryGetConsumer(string consumerTag, out IHaBasicConsumer consumer)
        {
            return _consumers.TryGetValue(consumerTag, out consumer);
        }

        public void Remove(string consumerTag)
        {
            _consumers.Remove(consumerTag);
        }

        public void ModelConnected(IModel model)
        {
            List<IHaBasicConsumer> consumers;
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_consumers.Count == 0)
                    return;

                if (_log.IsDebugEnabled)
                    _log.DebugFormat("Reconnecting {0} consumers on {1}", _consumers.Count, _model.Connection);

                consumers = _consumers.Values.ToList();
            }

            consumers.Each(x =>
            {
                if (_log.IsDebugEnabled)
                    _log.DebugFormat("Reconnecting {0} consumers on {1}", x.ConsumerTag, _model.Connection);

                model.BasicConsume(x.Queue, x.NoAck, x.ConsumerTag, x.NoLocal, x.Exclusive, x.Arguments, x);
            });
        }

        public void HandleModelDisposed()
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_consumers.Count == 0)
                    return;

                if (_log.IsDebugEnabled)
                    _log.DebugFormat("Shutting down {0} consumers on {1}", _consumers.Count, _model.Connection);

                var args = new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Ok");

                _consumers.Values.ToList().Each(x => x.HandleModelShutdown(_model, args));
                _consumers.Clear();
            }
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