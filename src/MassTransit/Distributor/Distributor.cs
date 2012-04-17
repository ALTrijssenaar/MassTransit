// Copyright 2007-2010 The Apache Software Foundation.
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
namespace MassTransit.Distributor
{
	using System;
	using System.Linq;
	using Magnum;
	using Magnum.Extensions;
	using Magnum.Fibers;
	using Magnum.Threading;
	using Messages;

	public class Distributor<T> :
		IDistributor<T>,
		Consumes<T>.Selected
		where T : class
	{
		private readonly IEndpointFactory _endpointFactory;
		private readonly IWorkerSelectionStrategy<T> _selectionStrategy;
		private readonly ReaderWriterLockedDictionary<Uri, WorkerDetails> _workers = new ReaderWriterLockedDictionary<Uri, WorkerDetails>();
		private Scheduler _threadPoolScheduler;
		private UnsubscribeAction _unsubscribeAction = () => false;
        private readonly int _pingTimeout = (int)5.Seconds().TotalMilliseconds;

		public Distributor(IEndpointFactory endpointFactory, IWorkerSelectionStrategy<T> workerSelectionStrategy)
		{
			_endpointFactory = endpointFactory;
			_selectionStrategy = workerSelectionStrategy;
		}

		public Distributor(IEndpointFactory endpointFactory) :
				this(endpointFactory, new DefaultWorkerSelectionStrategy<T>())
		{
		}

		public void Consume(T message)
		{
			WorkerDetails worker = _selectionStrategy.SelectWorker(_workers.Values, message);
			if (worker == null)
			{
				CurrentMessage.RetryLater();
				return;
			}

			worker.Add();

			IEndpoint endpoint = _endpointFactory.GetEndpoint(worker.DataUri);

			var distributed = new Distributed<T>(message, CurrentMessage.Headers.ResponseAddress);

			endpoint.Send(distributed);
		}

		public bool Accept(T message)
		{
			return _selectionStrategy.HasAvailableWorker(_workers.Values, message);
		}

		public void Dispose()
		{
		}

		public void Start(IServiceBus bus)
		{
			_unsubscribeAction = bus.ControlBus.Subscribe<WorkerAvailable<T>>(Consume);

			// don't plan to unsubscribe this since it's an important thing
			bus.Subscribe(this);

			_threadPoolScheduler = new TimerScheduler(new ThreadPoolFiber() );

			_threadPoolScheduler.Schedule(_pingTimeout, _pingTimeout, new ThreadPoolFiber(), PingWorkers);
		}

		public void Stop()
		{
			_threadPoolScheduler.Stop();

			_workers.Clear();

			_unsubscribeAction();
		}

		public void Consume(WorkerAvailable<T> message)
		{
			WorkerDetails worker = _workers.Retrieve(message.ControlUri, () =>
				{
					return new WorkerDetails
						{
							ControlUri = message.ControlUri,
							DataUri = message.DataUri,
							InProgress = message.InProgress,
							InProgressLimit = message.InProgressLimit,
							Pending = message.Pending,
							PendingLimit = message.PendingLimit,
							LastUpdate = message.Updated,
						};
				});

			worker.UpdateInProgress(message.InProgress, message.InProgressLimit, message.Pending, message.PendingLimit, message.Updated);
		}

		private void PingWorkers()
		{
			_workers.Values
				.Where(x => x.LastUpdate < SystemUtil.UtcNow.Subtract(_pingTimeout.Milliseconds()))
				.ToList()
				.ForEach(x => { _endpointFactory.GetEndpoint(x.ControlUri).Send(new PingWorker()); });
		}
	}
}