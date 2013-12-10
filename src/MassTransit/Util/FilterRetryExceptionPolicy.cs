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


    public class FilterRetryExceptionPolicy<T> :
        IRetryExceptionPolicy
        where T : Exception
    {
        Func<T, bool> _filter;

        public FilterRetryExceptionPolicy(Func<T, bool> filter)
        {
            _filter = filter;
        }

        public bool Filter(Exception exception)
        {
            var ex = exception as T;
            if (ex != null)
                return _filter(ex);

            return false;
        }
    }
}