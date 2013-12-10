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
    using System.Threading;
    using Logging;
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;
    using RabbitMQ.Client.Exceptions;
    using Util;

    // TODO create an HaConnectionProxy that actually proxies the connection so that
    // connections are shared

    // TODO insulate closing the connection proxy from closing the connection

    // TODO handle the closing events by setting _connection to null forcing a reconnect
    // on the next call that uses the connection

    public class HaConnection :
        IHaConnection
    {
        static readonly ILog _log = Logger.Get<HaConnection>();
        readonly ConnectionFactory _connectionFactory;
        readonly int _lockTimeout;
        readonly HashSet<IHaModel> _models;
        readonly object _monitor = new object();
        readonly RetryPolicy _retryPolicy;
        bool _connecting;
        IConnection _connection;
        bool _disposed;

        public HaConnection(ConnectionFactory connectionFactory, RetryPolicy retryPolicy, TimeSpan lockTimeout)
        {
            _lockTimeout = Convert.ToInt32(lockTimeout.TotalMilliseconds);
            _connectionFactory = connectionFactory;
            _retryPolicy = retryPolicy;

            _models = new HashSet<IHaModel>();
        }

        public void Dispose()
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_disposed)
                    return;

                if (_connection != null)
                {
                    _connection.Dispose();
                    _connection = null;

                    if (_log.IsDebugEnabled)
                    {
                        _log.DebugFormat("Connection closed: {0}", ToString());
                    }
                }

                _disposed = true;
            }
        }

        public IModel CreateModel()
        {
            return Execute(Retry.Allow<ChannelAllocationException>(), connection =>
                {
                    IModel model = connection.CreateModel();

                    var proxy = new HaModel(this, model, _lockTimeout, _retryPolicy);

                    _models.Add(proxy);

                    return proxy;
                });
        }

        public void Close()
        {
            Close(Timeout.Infinite);
        }

        public void Close(ushort reasonCode, string reasonText)
        {
            Close(reasonCode, reasonText, Timeout.Infinite);
        }

        public void Close(int timeout)
        {
            Close(200, "Goodbye", timeout);
        }

        public void Close(ushort reasonCode, string reasonText, int timeout)
        {
            Execute(connection => connection.Close(reasonCode, reasonText, timeout));
        }

        public void Abort()
        {
            Abort(Timeout.Infinite);
        }

        public void Abort(ushort reasonCode, string reasonText)
        {
            Abort(reasonCode, reasonText, Timeout.Infinite);
        }

        public void Abort(int timeout)
        {
            Abort(200, "Connection close forced", timeout);
        }

        public void Abort(ushort reasonCode, string reasonText, int timeout)
        {
            Execute(connection => connection.Abort(reasonCode, reasonText, timeout));
        }

        public void HandleConnectionBlocked(string reason)
        {
            throw new NotImplementedException();
        }

        public void HandleConnectionUnblocked()
        {
            throw new NotImplementedException();
        }

        public AmqpTcpEndpoint Endpoint
        {
            get { return Execute(x => x.Endpoint); }
        }

        public IProtocol Protocol
        {
            get { return Execute(x => x.Protocol); }
        }

        public ushort ChannelMax
        {
            get { return Execute(x => x.ChannelMax); }
        }

        public uint FrameMax
        {
            get { return Execute(x => x.FrameMax); }
        }

        public ushort Heartbeat
        {
            get { return Execute(x => x.Heartbeat); }
        }

        public IDictionary<string, object> ClientProperties
        {
            get { return Execute(x => x.ClientProperties); }
        }

        public IDictionary<string, object> ServerProperties
        {
            get { return Execute(x => x.ServerProperties); }
        }

        public AmqpTcpEndpoint[] KnownHosts
        {
            get { return Execute(x => x.KnownHosts); }
        }

        public ShutdownEventArgs CloseReason
        {
            get { return Execute(x => x.CloseReason); }
        }

        public bool IsOpen
        {
            get { return Execute(x => x.IsOpen); }
        }

        public bool AutoClose
        {
            get { return Execute(x => x.AutoClose); }
            set
            {
                // do not allow this as it makes the connection hard to track
            }
        }

        public IList<ShutdownReportEntry> ShutdownReport
        {
            get { return Execute(x => x.ShutdownReport); }
        }

        public event ConnectionShutdownEventHandler ConnectionShutdown
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event CallbackExceptionEventHandler CallbackException
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event ConnectionBlockedEventHandler ConnectionBlocked
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event ConnectionUnblockedEventHandler ConnectionUnblocked
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public void OnModelDisposed(IHaModel model)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_models.Remove(model))
                {
                    // model was removed, do any no model checking here??
                    // maybe set a disconnect timer?
                }
            }
        }

        public IModel RecreateModel()
        {
            return Execute(connection =>
                {
                    if (_log.IsDebugEnabled)
                        _log.DebugFormat("Recreating model for connection {0}", ToString());
                    return connection.CreateModel();
                });
        }

        public override string ToString()
        {
            return string.Format("HaConnection: {0}{1}", _connectionFactory.HostName, _connectionFactory.VirtualHost);
        }


        void Execute(Action<IConnection> callback)
        {
            Execute<object>(connection =>
                {
                    callback(connection);
                    return null;
                });
        }

        T Execute<T>(Func<IConnection, T> callback)
        {
            return Execute(Retry.All, callback);
        }

        T Execute<T>(IRetryExceptionPolicy retryExceptionPolicy, Func<IConnection, T> callback)
        {
            using (TimedLock.Lock(_monitor, _lockTimeout))
            {
                if (_connection == null)
                {
                    if (false == _connecting)
                    {
                        ThreadPool.QueueUserWorkItem(_ => Connect());
                        _connecting = true;
                    }
                }

                while (_connection == null)
                    Monitor.Wait(_monitor, _lockTimeout);

                if (_connection == null)
                    throw new LockTimeoutException();

                return _retryPolicy.Execute(retryExceptionPolicy, () => callback(_connection));
            }
        }

        /// <summary>
        /// Asynchronously connect to the RabbitMQ server using the connection factory
        /// </summary>
        void Connect()
        {
            try
            {
                using (TimedLock.Lock(_monitor, _lockTimeout))
                {
                    if (_disposed)
                    {
                        if (_log.IsDebugEnabled)
                        {
                            _log.DebugFormat("Connection was disposed before connection was established: {0}",
                                ToString());
                        }
                        return;
                    }

                    try
                    {
                        _connection = _retryPolicy.Execute(() =>
                            {
                                if (_log.IsDebugEnabled)
                                {
                                    _log.DebugFormat("Connecting to RabbitMQ: {0}", ToString());
                                }

                                return _connectionFactory.CreateConnection();
                            });
                    }
                    finally
                    {
                        _connecting = false;
                    }

                    Monitor.PulseAll(_monitor);
                }
            }
            catch (Exception ex)
            {
                if (_log.IsErrorEnabled)
                {
                    _log.Error(string.Format("Failed to connect to RabbitMQ: {0}", ToString()), ex);
                }
            }
        }
    }
}