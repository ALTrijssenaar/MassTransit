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
    using System.Threading;
    using Logging;
    using Magnum.Extensions;
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;
    using RabbitMQ.Client.Exceptions;
    using Util;


    public class HaModel :
        IHaModel
    {
        static readonly IRetryExceptionPolicy DeclareExchangeExceptions =
            new FilterRetryExceptionPolicy<OperationInterruptedException>(x => x.ShutdownReason.ReplyCode == 406);

        static readonly ILog _log = Logger.Get<HaModel>();

        readonly IHaConnection _connection;
        readonly HaBasicConsumerList _consumers;
        readonly IDictionary<BindingKey, BoundExchange> _exchangeBindings;
        readonly IDictionary<string, DeclaredExchange> _exchanges;
        readonly int _lockTimeout;
        readonly object _monitor = new object();
        readonly IDictionary<BindingKey, BoundQueue> _queueBindings;
        readonly IDictionary<string, DeclaredQueue> _queues;
        readonly RetryPolicy _retryPolicy;
        readonly SortedList<ulong, PendingBasicPublish> _unconfirmed;

        bool _creatingModel;
        bool _disposed;
        IModel _model;
        ushort _prefetchCount;
        uint _prefetchSize;


        public HaModel(IHaConnection connection, IModel model, int lockTimeout, RetryPolicy retryPolicy)
        {
            _connection = connection;
            _lockTimeout = lockTimeout;
            _retryPolicy = retryPolicy;

            _unconfirmed = new SortedList<ulong, PendingBasicPublish>();

            _exchanges = new Dictionary<string, DeclaredExchange>();
            _queues = new Dictionary<string, DeclaredQueue>();
            _exchangeBindings = new Dictionary<BindingKey, BoundExchange>();
            _queueBindings = new Dictionary<BindingKey, BoundQueue>();

            _consumers = new HaBasicConsumerList(this, _lockTimeout);

            if (_log.IsDebugEnabled)
                _log.DebugFormat("Model created: {0}", connection);

            _model = model;

            ConfigureModel(model);
        }

        public void Dispose()
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_disposed)
                    return;

                if (_log.IsDebugEnabled)
                    _log.DebugFormat("Disposing of model on connection {0}", _connection);

                _consumers.HandleModelDisposed();

                if (_model != null)
                {
                    DetachModel(_model);

                    _model.Dispose();
                    _model = null;
                }

                _connection.OnModelDisposed(this);
                _disposed = true;
            }
        }

        public IBasicProperties CreateBasicProperties()
        {
            return Execute(model => model.CreateBasicProperties());
        }

        public IFileProperties CreateFileProperties()
        {
            return Execute(model => model.CreateFileProperties());
        }

        public IStreamProperties CreateStreamProperties()
        {
            return Execute(model => model.CreateStreamProperties());
        }

        public void ChannelFlow(bool active)
        {
            Execute(model => model.ChannelFlow(active));
        }

        public void ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            Execute(DeclareExchangeExceptions, model =>
                {
                    var declaredExchange = new DeclaredExchange(exchange, type, durable, autoDelete, arguments);

                    DeclaredExchange existingExchange;
                    if (_exchanges.TryGetValue(exchange, out existingExchange))
                    {
                        if (DeclaredExchange.DeclaredExchangeComparer.Equals(declaredExchange, existingExchange))
                            return;
                    }

                    if (_log.IsDebugEnabled)
                        _log.DebugFormat("Declaring exchange: {0}", declaredExchange);

                    model.ExchangeDeclare(exchange, type, durable, autoDelete, arguments);

                    _exchanges[exchange] = declaredExchange;
                });
        }

        public void ExchangeDeclare(string exchange, string type, bool durable)
        {
            ExchangeDeclare(exchange, type, durable, false, null);
        }

        public void ExchangeDeclare(string exchange, string type)
        {
            ExchangeDeclare(exchange, type, false);
        }

        public void ExchangeDeclarePassive(string exchange)
        {
            Execute(model => model.ExchangeDeclarePassive(exchange));
        }

        public void ExchangeDelete(string exchange, bool ifUnused)
        {
            Execute(model =>
                {
                    if (_log.IsDebugEnabled)
                        _log.DebugFormat("Deleting exchange: {0}({1})", exchange, ifUnused);

                    model.ExchangeDelete(exchange, ifUnused);

                    _exchanges.Remove(exchange);

                    _exchangeBindings
                        .Where(x => x.Key.Source == exchange || x.Key.Destination == exchange)
                        .Select(x => x.Key)
                        .ToList()
                        .Each(x => _exchangeBindings.Remove(x));
                });
        }

        public void ExchangeDelete(string exchange)
        {
            ExchangeDelete(exchange, false);
        }

        public void ExchangeBind(string destination, string source, string routingKey,
            IDictionary<string, object> arguments)
        {
            Execute(model =>
                {
                    var key = new BindingKey(destination, source);
                    var binding = new BoundExchange(destination, source, routingKey, arguments);

                    BoundExchange existingBinding;
                    if (_exchangeBindings.TryGetValue(key, out existingBinding))
                    {
                        if (existingBinding == binding)
                            return;
                    }

                    model.ExchangeBind(destination, source, routingKey, arguments);

                    _exchangeBindings[key] = binding;
                });
        }

        public void ExchangeBind(string destination, string source, string routingKey)
        {
            ExchangeBind(destination, source, routingKey, null);
        }

        public void ExchangeUnbind(string destination, string source, string routingKey,
            IDictionary<string, object> arguments)
        {
            Execute(model =>
                {
                    model.ExchangeUnbind(destination, source, routingKey, arguments);

                    var key = new BindingKey(destination, source);

                    _exchangeBindings.Remove(key);
                });
        }

        public void ExchangeUnbind(string destination, string source, string routingKey)
        {
            ExchangeUnbind(destination, source, routingKey, null);
        }

        public QueueDeclareOk QueueDeclare()
        {
            return QueueDeclare("", false, true, true, null);
        }

        public QueueDeclareOk QueueDeclarePassive(string queue)
        {
            return Execute(model => model.QueueDeclarePassive(queue));
        }

        public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            return Execute(model =>
                {
                    QueueDeclareOk queueDeclareOk = model.QueueDeclare(queue, durable, exclusive, autoDelete, arguments);

                    var declaredQueue = new DeclaredQueue(queueDeclareOk.QueueName, durable, exclusive, autoDelete,
                        arguments);

                    DeclaredQueue existingQueue;
                    if (_queues.TryGetValue(queue, out existingQueue))
                    {
                        if (existingQueue == declaredQueue)
                            return queueDeclareOk;
                    }

                    _queues[queue] = declaredQueue;

                    return queueDeclareOk;
                });
        }

        public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            Execute(model =>
                {
                    var key = new BindingKey(queue, exchange);
                    var binding = new BoundQueue(queue, exchange, routingKey, arguments);

                    BoundQueue existingBinding;
                    if (_queueBindings.TryGetValue(key, out existingBinding))
                    {
                        if (existingBinding == binding)
                            return;
                    }

                    model.QueueBind(queue, exchange, routingKey, arguments);

                    _queueBindings[key] = binding;
                });
        }

        public void QueueBind(string queue, string exchange, string routingKey)
        {
            QueueBind(queue, exchange, routingKey, null);
        }

        public void QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            Execute(model =>
                {
                    model.QueueUnbind(queue, exchange, routingKey, arguments);

                    var key = new BindingKey(queue, exchange);

                    _queueBindings.Remove(key);
                });
        }

        public uint QueuePurge(string queue)
        {
            return Execute(model => model.QueuePurge(queue));
        }

        public uint QueueDelete(string queue, bool ifUnused, bool ifEmpty)
        {
            return Execute(model =>
                {
                    uint result = model.QueueDelete(queue, ifUnused, ifEmpty);

                    _queues.Remove(queue);

                    _queueBindings
                        .Keys.Where(x => x.Destination == queue)
                        .Select(x => new BindingKey(x.Destination, x.Source))
                        .ToList()
                        .Each(x => _queueBindings.Remove(x));

                    return result;
                });
        }

        public uint QueueDelete(string queue)
        {
            return QueueDelete(queue, false, false);
        }

        public void ConfirmSelect()
        {
            // this is always on for HaModel types
        }

        public bool WaitForConfirms()
        {
            return Execute(model => model.WaitForConfirms());
        }

        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            throw new NotImplementedException();
        }

        public void WaitForConfirmsOrDie()
        {
            throw new NotImplementedException();
        }

        public void WaitForConfirmsOrDie(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public string BasicConsume(string queue, bool noAck, IBasicConsumer consumer)
        {
            return BasicConsume(queue, noAck, "", consumer);
        }

        public string BasicConsume(string queue, bool noAck, string consumerTag, IBasicConsumer consumer)
        {
            return BasicConsume(queue, noAck, consumerTag, false, false, null, consumer);
        }

        public string BasicConsume(string queue, bool noAck, string consumerTag, IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            return BasicConsume(queue, noAck, consumerTag, false, false, arguments, consumer);
        }

        public string BasicConsume(string queue, bool noAck, string consumerTag, bool noLocal, bool exclusive,
            IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            return Execute(model =>
                {
                    if (_log.IsDebugEnabled)
                    {
                        _log.DebugFormat("Registering BasicConsumer {0} to queue {1} on connection {2}", consumerTag,
                            queue, _connection);
                    }

                    var basicConsumer = new HaBasicConsumer(_consumers, queue, noAck, consumerTag, noLocal, exclusive,
                        arguments,
                        consumer);

                    string actualConsumerTag = model.BasicConsume(queue, noAck, consumerTag, noLocal, exclusive,
                        arguments, basicConsumer);

                    if (_log.IsDebugEnabled)
                    {
                        _log.DebugFormat("BasicConsumer registered {0} to queue {1} on connection {2}",
                            actualConsumerTag,
                            queue, _connection);
                    }

                    return actualConsumerTag;
                });
        }

        public void BasicCancel(string consumerTag)
        {
            Execute(model =>
                {
                    if (_log.IsDebugEnabled)
                        _log.DebugFormat("Canceling consumer {0} on connection {1}", consumerTag, _connection);

                    IHaBasicConsumer consumer;
                    if (_consumers.TryGetConsumer(consumerTag, out consumer))
                    {
                        model.BasicCancel(consumerTag);

                        if (_log.IsDebugEnabled)
                            _log.DebugFormat("Consumer canceled {0} on connection {1}", consumerTag, _connection);
                    }
                    else
                    {
                        if (_log.IsDebugEnabled)
                            _log.DebugFormat("Consumer tag {0} not found on connection {1}", consumerTag, _connection);
                    }
                });
        }

        public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            Execute(model =>
                {
                    model.BasicQos(prefetchSize, prefetchCount, false);

                    _prefetchSize = prefetchSize;
                    _prefetchCount = prefetchCount;
                });
        }

        public void BasicPublish(PublicationAddress addr, IBasicProperties basicProperties, byte[] body)
        {
            BasicPublish(addr.ExchangeName, addr.RoutingKey, basicProperties, body);
        }

        public void BasicPublish(string exchange, string routingKey, IBasicProperties basicProperties, byte[] body)
        {
            BasicPublish(exchange, routingKey, false, basicProperties, body);
        }

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties,
            byte[] body)
        {
            BasicPublish(exchange, routingKey, mandatory, false, basicProperties, body);
        }


        public void BasicPublish(string exchange, string routingKey, bool mandatory, bool immediate,
            IBasicProperties basicProperties, byte[] body)
        {
            PendingBasicPublish pending = Execute(model =>
                {
                    if (basicProperties == null)
                        basicProperties = CreateBasicProperties();

                    ulong next = model.NextPublishSeqNo;
                    var pendingBasicPublish = new PendingBasicPublish(next, exchange, routingKey, mandatory, immediate,
                        basicProperties, body);

                    model.BasicPublish(exchange, routingKey, mandatory, immediate, basicProperties, body);

                    _unconfirmed.Add(next, pendingBasicPublish);

                    return pendingBasicPublish;
                });

            pending.Wait(_lockTimeout);
        }

        public void BasicAck(ulong deliveryTag, bool multiple)
        {
            Execute(model => { model.BasicAck(deliveryTag, multiple); });
        }

        public void BasicReject(ulong deliveryTag, bool requeue)
        {
            Execute(model => { model.BasicReject(deliveryTag, requeue); });
        }

        public void BasicNack(ulong deliveryTag, bool multiple, bool requeue)
        {
            Execute(model => { model.BasicNack(deliveryTag, multiple, requeue); });
        }

        public void BasicRecover(bool requeue)
        {
            Execute(model => { model.BasicRecover(requeue); });
        }

        public void BasicRecoverAsync(bool requeue)
        {
            Execute(model => { model.BasicRecoverAsync(requeue); });
        }

        public BasicGetResult BasicGet(string queue, bool noAck)
        {
            return Execute(model => model.BasicGet(queue, noAck));
        }

        public void TxSelect()
        {
            Execute(model => model.TxSelect());
        }

        public void TxCommit()
        {
            Execute(model => model.TxCommit());
        }

        public void TxRollback()
        {
            Execute(model => model.TxRollback());
        }

        public void DtxSelect()
        {
            Execute(model => model.DtxSelect());
        }

        public void DtxStart(string dtxIdentifier)
        {
            Execute(model => model.DtxStart(dtxIdentifier));
        }

        public void Close()
        {
            Execute(model => model.Close());
        }

        public void Close(ushort replyCode, string replyText)
        {
            Execute(model => model.Close(replyCode, replyText));
        }

        public void Abort()
        {
            Execute(model => model.Abort());
        }

        public void Abort(ushort replyCode, string replyText)
        {
            Execute(model => model.Abort(replyCode, replyText));
        }

        public IBasicConsumer DefaultConsumer
        {
            get { throw new NotImplementedException("Default consumer is not supported"); }
            set { throw new NotImplementedException("Default consumer is not supported"); }
        }

        public ShutdownEventArgs CloseReason
        {
            get { return Execute(model => model.CloseReason); }
        }

        public bool IsOpen
        {
            get { return Execute(model => model.IsOpen); }
        }

        public ulong NextPublishSeqNo
        {
            get { return Execute(model => model.NextPublishSeqNo); }
        }

        public event ModelShutdownEventHandler ModelShutdown
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event BasicReturnEventHandler BasicReturn
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event BasicAckEventHandler BasicAcks
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event BasicNackEventHandler BasicNacks
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event CallbackExceptionEventHandler CallbackException
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event FlowControlEventHandler FlowControl
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event BasicRecoverOkEventHandler BasicRecoverOk
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public IHaConnection Connection
        {
            get { return _connection; }
        }

        void Execute(Action<IModel> callback)
        {
            Execute<object>(connection =>
                {
                    callback(connection);
                    return null;
                });
        }

        void Execute(IRetryExceptionPolicy retryExceptionPolicy, Action<IModel> callback)
        {
            Execute<object>(retryExceptionPolicy, connection =>
                {
                    callback(connection);
                    return null;
                });
        }

        T Execute<T>(Func<IModel, T> callback)
        {
            return Execute(Retry.All, callback);
        }

        T Execute<T>(IRetryExceptionPolicy retryExceptionPolicy, Func<IModel, T> callback)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_model == null)
                    CreateModelAsync();

                while (_model == null)
                    Monitor.Wait(_monitor, _lockTimeout);

                if (_model == null)
                    throw new LockTimeoutException();

                return _retryPolicy.Execute(retryExceptionPolicy, () => callback(_model));
            }
        }

        void CreateModelAsync()
        {
            if (false == _creatingModel)
            {
                ThreadPool.QueueUserWorkItem(_ => CreateModel());
                _creatingModel = true;
            }
        }

        void CreateModel()
        {
            try
            {
                using (TimedLock.Lock(_monitor, _lockTimeout))
                {
                    if (_disposed)
                        return;

                    if (_creatingModel == false)
                        return;

                    try
                    {
                        if (_model != null)
                        {
                            _log.DebugFormat("CreateModel requested but model already exists");
                            return;
                        }

                        _model = _retryPolicy.Execute(() =>
                            {
                                IModel model = _connection.RecreateModel();

                                ConfigureModel(model);

                                return model;
                            });

                        _consumers.ModelConnected(_model);
                    }
                    finally
                    {
                        _creatingModel = false;
                    }

                    Monitor.PulseAll(_monitor);
                }
            }
            catch (Exception ex)
            {
                if (_log.IsErrorEnabled)
                    _log.Error(string.Format("Failed to create model: {0}", _connection), ex);
            }
        }

        void ConfigureModel(IModel model)
        {
            model.ModelShutdown += HandleModelShutdown;
            model.BasicReturn += HandleBasicReturn;
            model.BasicAcks += HandleBasicAcks;
            model.BasicNacks += HandleBasicNacks;
            model.CallbackException += HandleCallbackException;
            model.FlowControl += HandleFlowControl;
            model.BasicRecoverOk += HandleBasicRecoverOk;

            model.ConfirmSelect();

            if (_prefetchSize > 0 || _prefetchCount > 0)
                model.BasicQos(_prefetchSize, _prefetchCount, false);

            _exchanges.Values.Each(x => model.ExchangeDeclare(x.Exchange, x.Type, x.Durable, x.AutoDelete, x.Arguments));

            _exchangeBindings.Values.Each(x => model.ExchangeBind(x.Destination, x.Source, x.RoutingKey, x.Arguments));

            _queues.Values.Each(x => model.QueueDeclare(x.QueueName, x.Durable, x.Exclusive, x.AutoDelete, x.Arguments));

            _queueBindings.Values.Each(x => model.QueueBind(x.Queue, x.Exchange, x.RoutingKey, x.Arguments));
        }


        void DetachModel(IModel model)
        {
            model.ModelShutdown -= HandleModelShutdown;
            model.BasicReturn -= HandleBasicReturn;
            model.BasicAcks -= HandleBasicAcks;
            model.BasicNacks -= HandleBasicNacks;
            model.CallbackException -= HandleCallbackException;
            model.FlowControl -= HandleFlowControl;
            model.BasicRecoverOk -= HandleBasicRecoverOk;
        }


        void HandleBasicRecoverOk(IModel model, EventArgs args)
        {
            throw new NotImplementedException();
        }

        void HandleFlowControl(IModel sender, FlowControlEventArgs args)
        {
            throw new NotImplementedException();
        }

        void HandleCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            throw new NotImplementedException();
        }

        void HandleBasicNacks(IModel model, BasicNackEventArgs args)
        {
            throw new NotImplementedException();
        }

        void HandleBasicAcks(IModel model, BasicAckEventArgs args)
        {
            Execute(m =>
                {
                    if (args.Multiple)
                    {
                        int lastIndex;
                        if (_unconfirmed.ContainsKey(args.DeliveryTag))
                            lastIndex = _unconfirmed.IndexOfKey(args.DeliveryTag);
                        else if (_unconfirmed.Any(x => x.Key < args.DeliveryTag))
                            lastIndex = _unconfirmed.IndexOfKey(_unconfirmed.Last(x => x.Key < args.DeliveryTag).Key);
                        else
                            return;

                        AckPendingPublish(0, lastIndex);
                    }
                    else
                    {
                        int index = _unconfirmed.IndexOfKey(args.DeliveryTag);
                        AckPendingPublish(index, index);
                    }
                });
        }

        void AckPendingPublish(int first, int last)
        {
            for (int i = last; i >= first; i--)
            {
                PendingBasicPublish pending = _unconfirmed.Values[i];
                pending.Ack();
                _unconfirmed.RemoveAt(i);
            }
        }

        void HandleBasicReturn(IModel model, BasicReturnEventArgs args)
        {
            throw new NotImplementedException();
        }

        void HandleModelShutdown(IModel model, ShutdownEventArgs reason)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_log.IsDebugEnabled)
                {
                    _log.DebugFormat("ModelShutdown on connection: {3}, {0} - {1} ({2})", reason.ReplyCode,
                        reason.ReplyText, reason.Initiator, _connection);
                }

                DetachModel(model);

                _model = null;

                // to keep consumers alive, the model should be recreated so that it is reattached
                if (_consumers.Count > 0)
                    CreateModelAsync();
            }
        }
    }
}