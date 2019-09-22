using System;
using System.Collections.Generic;
using System.Threading;

namespace GZipTest
{
    internal class SimpleBlockingQueue<T>
    {
        private bool _allProducersStopped = false;
        private int _liveProducers;
        private readonly Queue<T> _queue;
        private int _waitingConsumers = 0;

        public SimpleBlockingQueue(int liveProducers)
        {
            _liveProducers = liveProducers;
            _queue = new Queue<T>();
        }

        internal SimpleBlockingQueue(T[] initialValues, int liveProducers) : this(liveProducers)
        {
            if (initialValues == null) throw new ArgumentNullException(nameof(initialValues));
            _queue = new Queue<T>(initialValues);
        }

        internal T Dequeue()
        {
            lock (_queue)
            {
                if (_queue.Count > 0)
                    return _queue.Dequeue();

                while (_queue.Count == 0 && !_allProducersStopped)
                {
                    _waitingConsumers++;
                    try
                    {
                        Monitor.Wait(_queue);
                        if (_queue.Count > 0)
                        {
                            var dequeue = _queue.Dequeue();
                            Monitor.Pulse(_queue);
                            return dequeue;
                        }
                        if (_allProducersStopped)
                        {
                            Monitor.Pulse(_queue);
                            return default(T);
                        }
                    }
                    finally
                    {
                        _waitingConsumers--;
                    }
                }
                return default(T);
            }
        }

        internal bool Enqueue(T item)
        {
            lock (_queue)
            {
                if (_allProducersStopped)
                    return false;
                _queue.Enqueue(item);

                if (_waitingConsumers > 0)
                    Monitor.Pulse(_queue);
            }
            return true;
        }

        internal void StopProducer()
        {
            lock (_queue)
                if (_liveProducers > 0)
                {
                    _liveProducers--;
                    if (_liveProducers == 0)
                    {
                        _allProducersStopped = true;
                        Monitor.Pulse(_queue);
                    }
                }
        }
    }
}