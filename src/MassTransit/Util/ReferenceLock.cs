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
namespace MassTransit.Util
{
    using System;
    using System.Threading;


    public class ReferenceLock<T>
        where T : class
    {
        readonly object _monitor;
        readonly int _timeout;
        T _reference;

        public ReferenceLock(int timeout)
        {
            _monitor = new object();
            _timeout = timeout;
        }

        public bool HasValue
        {
            get
            {
                using (TimedLock.Lock(_monitor, TimeSpan.Zero))
                {
                    return _reference != null;
                }
            }
        }

        public void Set(T reference)
        {
            using (TimedLock.Lock(_monitor, _timeout))
            {
                _reference = reference;
                Monitor.PulseAll(_monitor);
            }
        }

        public void Use(Action<T> callback)
        {
            using (TimedLock.Lock(_monitor, _timeout))
            {
                while (_reference == null)
                    Monitor.Wait(_monitor, _timeout);

                if (_reference == null)
                    throw new LockTimeoutException();

                callback(_reference);
            }
        }

        public TResult Use<TResult>(Func<T, TResult> callback)
        {
            using (TimedLock.Lock(_monitor, _timeout))
            {
                while (_reference == null)
                    Monitor.Wait(_monitor, _timeout);

                if (_reference == null)
                    throw new LockTimeoutException();

                return callback(_reference);
            }
        }
    }


    public static class ReferenceLock
    {
        public static ReferenceLock<T> Create<T>(TimeSpan timeout)
            where T : class
        {
            return new ReferenceLock<T>(Convert.ToInt32(timeout.TotalMilliseconds));
        }

        public static ReferenceLock<T> Create<T>(int timeout)
            where T : class
        {
            return new ReferenceLock<T>(timeout);
        }
    }
}