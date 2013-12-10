namespace MassTransit.Transports.RabbitMq.HaClient
{
    using System;
    using System.Collections.Generic;


    public class BoundExchange : 
        IEquatable<BoundExchange>
    {
        public readonly IDictionary<string, object> Arguments;
        public readonly string Destination;
        public readonly string RoutingKey;
        public readonly string Source;

        public BoundExchange(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            Destination = destination;
            Source = source;
            RoutingKey = routingKey;
            Arguments = arguments;
        }

        public bool Equals(BoundExchange other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;
            return string.Equals(Destination, other.Destination) && string.Equals(RoutingKey, other.RoutingKey)
                   && string.Equals(Source, other.Source);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((BoundExchange)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (Destination != null ? Destination.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (RoutingKey != null ? RoutingKey.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Source != null ? Source.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(BoundExchange left, BoundExchange right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(BoundExchange left, BoundExchange right)
        {
            return !Equals(left, right);
        }
    }
}