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


    public class BoundQueue :
        IEquatable<BoundQueue>
    {
        public readonly IDictionary<string, object> Arguments;
        public readonly string Queue;
        public readonly string RoutingKey;
        public readonly string Exchange;

        public BoundQueue(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            Queue = queue;
            Exchange = exchange;
            RoutingKey = routingKey;
            Arguments = arguments;
        }

        public bool Equals(BoundQueue other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;
            return string.Equals(Queue, other.Queue) && string.Equals(RoutingKey, other.RoutingKey)
                   && string.Equals(Exchange, other.Exchange);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((BoundQueue)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (Queue != null ? Queue.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (RoutingKey != null ? RoutingKey.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Exchange != null ? Exchange.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(BoundQueue left, BoundQueue right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(BoundQueue left, BoundQueue right)
        {
            return !Equals(left, right);
        }
    }
}