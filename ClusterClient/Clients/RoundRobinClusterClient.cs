using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var replicaTimeout = TimeSpan.FromMilliseconds(timeout.TotalMilliseconds / ReplicaAddresses.Length);
            var order = Enumerable.Range(0, ReplicaAddresses.Length).ToList();
            order.Shuffle();

            var tasks = order
                .Select(n => ReplicaAddresses[n])
                .Select(uri => CreateRequest($"{uri}?query={query}"))
                .Select(request =>
                {
                    Log.InfoFormat($"Processing {request.RequestUri}");
                    return ProcessRequestAsync(request);
                });

            foreach (var task in tasks)
            {
                await Task.WhenAny(task, Task.Delay(replicaTimeout));
                if (task.IsCompleted)
                    return task.Result;
            }
            
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}