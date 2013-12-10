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


    public class DeclaredQueue : 
        IEquatable<DeclaredQueue>
    {
        public readonly IDictionary<string, object> Arguments;
        public readonly bool AutoDelete;
        public readonly bool Durable;
        public readonly bool Exclusive;
        public readonly string QueueName;

        public DeclaredQueue(string queueName, bool durable, bool exclusive, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            QueueName = queueName;
            Durable = durable;
            Exclusive = exclusive;
            AutoDelete = autoDelete;
            Arguments = arguments;
        }

        public bool Equals(DeclaredQueue other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;
            return AutoDelete.Equals(other.AutoDelete) && Durable.Equals(other.Durable)
                   && Exclusive.Equals(other.Exclusive) && string.Equals(QueueName, other.QueueName);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((DeclaredQueue)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = AutoDelete.GetHashCode();
                hashCode = (hashCode * 397) ^ Durable.GetHashCode();
                hashCode = (hashCode * 397) ^ Exclusive.GetHashCode();
                hashCode = (hashCode * 397) ^ (QueueName != null ? QueueName.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(DeclaredQueue left, DeclaredQueue right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(DeclaredQueue left, DeclaredQueue right)
        {
            return !Equals(left, right);
        }
    }
}