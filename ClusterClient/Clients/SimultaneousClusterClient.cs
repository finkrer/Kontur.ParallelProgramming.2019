using System;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SimultaneousClusterClient : ClusterClientBase
    {
        public SimultaneousClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var tasks = ReplicaAddresses
                .Select(uri => CreateRequest($"{uri}?query={query}"))
                .Select(request =>
                {
                    Log.InfoFormat($"Processing {request.RequestUri}");
                    return ProcessRequestAsync(request);
                })
                .ToArray();

            var resultTask = Task.WhenAny(tasks);
            await Task.WhenAny(resultTask, Task.Delay(timeout));
            
            if (!resultTask.IsCompleted)
                throw new TimeoutException();

            return resultTask.Result.Result;
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SimultaneousClusterClient));
    }
}