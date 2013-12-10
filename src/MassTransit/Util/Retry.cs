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
    using System.Collections.Generic;
    using System.Threading;
    using Logging;


    public static class Retry
    {
        public static readonly IRetryExceptionPolicy All = new RetryAllRetryExceptionPolicy();
        static readonly ILog _log = Logger.Get<RetryPolicy>();

        public static void Execute(this RetryPolicy retryPolicy, Action action)
        {
            Execute(retryPolicy, All, () =>
                {
                    action();
                    return default(int);
                });
        }

        public static void Execute(this RetryPolicy retryPolicy, IRetryExceptionPolicy policy, Action action)
        {
            Execute(retryPolicy, policy, () =>
                {
                    action();
                    return default(int);
                });
        }

        public static T Execute<T>(this RetryPolicy retryPolicy, Func<T> action)
        {
            return Execute(retryPolicy, All, action);
        }

        public static T Execute<T>(this RetryPolicy retryPolicy, IRetryExceptionPolicy policy, Func<T> action)
        {
            using (IEnumerator<TimeSpan> retryInterval = retryPolicy.GetRetryIntervals.GetEnumerator())
            {
                for (int attempt = 0;; attempt++)
                {
                    try
                    {
                        if (attempt > 0)
                            Thread.Sleep(retryInterval.Current);

                        return action();
                    }
                    catch (Exception ex)
                    {
                        if (!retryInterval.MoveNext() || policy.Filter(ex))
                            throw;

                        if (_log.IsDebugEnabled)
                        {
                            _log.Debug(string.Format("Retry attempt {0} failed, retrying in {1}s", attempt,
                                retryInterval.Current.TotalSeconds), ex);
                        }
                    }
                }
            }
        }

        public static IRetryExceptionPolicy Allow<T>()
            where T : Exception
        {
            return new RetryExceptRetryExceptionPolicy(typeof(T));
        }

        public static IRetryExceptionPolicy Allow<T1, T2>()
            where T1 : Exception
            where T2 : Exception
        {
            return new RetryExceptRetryExceptionPolicy(typeof(T1), typeof(T2));
        }

        public static IRetryExceptionPolicy Allow<T1, T2, T3>()
            where T1 : Exception
            where T2 : Exception
            where T3 : Exception
        {
            return new RetryExceptRetryExceptionPolicy(typeof(T1), typeof(T2), typeof(T3));
        }
    }
}