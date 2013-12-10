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
    using System.Text;


    public class DeclaredExchange :
        IEquatable<DeclaredExchange>
    {
        static readonly IEqualityComparer<DeclaredExchange> DeclaredExchangeComparerInstance =
            new DeclaredExchangeEqualityComparer();


        public readonly IDictionary<string, object> Arguments;
        public readonly bool AutoDelete;
        public readonly bool Durable;
        public readonly string Exchange;
        public readonly string Type;

        public DeclaredExchange(string exchange, string type, bool durable, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            Exchange = exchange;
            Type = type;
            Durable = durable;
            AutoDelete = autoDelete;
            Arguments = arguments;
        }

        public override string ToString()
        {
            var sb = new StringBuilder(Exchange);

            sb.AppendFormat(" ({0})", Type);
            if (Durable)
                sb.Append(", (durable)");
            if (AutoDelete)
                sb.Append(", (auto-delete)");
            if(Arguments != null)
                foreach (var argument in Arguments)
                {
                    sb.AppendFormat(", ({0}: {1})", argument.Key, argument.Value);
                }

            return sb.ToString();
        }
        public static IEqualityComparer<DeclaredExchange> DeclaredExchangeComparer
        {
            get { return DeclaredExchangeComparerInstance; }
        }

        public bool Equals(DeclaredExchange other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;
            return string.Equals(Exchange, other.Exchange);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((DeclaredExchange)obj);
        }

        public override int GetHashCode()
        {
            return (Exchange != null ? Exchange.GetHashCode() : 0);
        }

        sealed class DeclaredExchangeEqualityComparer :
            IEqualityComparer<DeclaredExchange>
        {
            public bool Equals(DeclaredExchange x, DeclaredExchange y)
            {
                if (ReferenceEquals(x, y))
                    return true;
                if (ReferenceEquals(x, null))
                    return false;
                if (ReferenceEquals(y, null))
                    return false;
                if (x.GetType() != y.GetType())
                    return false;
                return x.AutoDelete.Equals(y.AutoDelete) && x.Durable.Equals(y.Durable)
                       && string.Equals(x.Exchange, y.Exchange) && string.Equals(x.Type, y.Type);
            }

            public int GetHashCode(DeclaredExchange obj)
            {
                unchecked
                {
                    int hashCode = 0;
                    hashCode = (hashCode * 397) ^ obj.AutoDelete.GetHashCode();
                    hashCode = (hashCode * 397) ^ obj.Durable.GetHashCode();
                    hashCode = (hashCode * 397) ^ (obj.Exchange != null ? obj.Exchange.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.Type != null ? obj.Type.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }
    }
}