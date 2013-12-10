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


    public struct TimedLock :
        IDisposable
    {
        readonly object _monitor;

        TimedLock(object monitor)
        {
            _monitor = monitor;
        }

        public void Dispose()
        {
            Monitor.Exit(_monitor);
        }

        public static TimedLock Lock(object monitor, TimeSpan timeout)
        {
            return Lock(monitor, Convert.ToInt32(timeout.TotalMilliseconds));
        }

        public static TimedLock Lock(object monitor, int milliseconds)
        {
            var timedLock = new TimedLock(monitor);
            if (!Monitor.TryEnter(monitor, milliseconds))
                throw new LockTimeoutException();

            return timedLock;
        }
    }
}