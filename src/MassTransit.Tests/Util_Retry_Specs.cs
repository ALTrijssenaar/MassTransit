namespace MassTransit.Tests
{
    using System;
    using System.Threading;
    using Magnum.Extensions;
    using NUnit.Framework;
    using Util;


    [TestFixture]
    public class Specifying_a_retry_policy
    {
        [Test]
        public void Should_try_the_specified_attempts()
        {
            var retryPolicy = new FixedIntervalRetryPolicy(3, 10.Milliseconds());

            int attempts = 0;

            Assert.Throws<InvalidOperationException>(() => retryPolicy.Execute(() =>
                {
                    attempts++;
                    throw new InvalidOperationException("Oh my");
                }));

            Assert.AreEqual(4, attempts);
        }


        [Test]
        public void Should_not_retry_an_excepted_exception()
        {
            var retryPolicy = new FixedIntervalRetryPolicy(3, 10.Milliseconds());

            int attempts = 0;

            Assert.Throws<InvalidOperationException>(() => retryPolicy.Execute(Retry.Allow<InvalidOperationException>(), () =>
            {
                attempts++;
                throw new InvalidOperationException("Oh my");
            }));

            Assert.AreEqual(1, attempts);
        }

        [Test]
        public void Should_use_a_resource_once_it_becomes_available()
        {
            object name = "Chris";

            var refLock = ReferenceLock.Create<object>(1.Hours());

            ThreadPool.QueueUserWorkItem(_ =>
                {
                    Thread.Sleep(1.Seconds());

                    refLock.Set(name);
                });

            object value = null;
            refLock.Use(x => value = x);

            Assert.AreEqual("Chris", value);
        }
    }
}
