using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cluster;
using ClusterClient.Clients;
using FluentAssertions;
using log4net;
using NUnit.Framework;

namespace ClusterTests
{
    [TestFixture("random")]
    [TestFixture("simultaneous")]
    [TestFixture("round_robin")]
    [TestFixture("smart")]
    internal class ClusterTest
    {
        private readonly string clientName;

        public ClusterTest(string clientName)
        {
            this.clientName = clientName;

//            Console.WriteLine(ThreadPool.SetMaxThreads(1024, 1024));
//            Console.WriteLine(ThreadPool.SetMinThreads(1024, 1024));
        }

        [SetUp]
        public void SetUp()
        {
            clusterServers = new List<ClusterServer>();

            log = LogManager.GetLogger(typeof(Program));
        }

        [TearDown]
        public void TearDown()
        {
            StopServers();
        }

        [Test]
        public void Client_should_return_success_when_there_is_only_one_good_replica()
        {
            StartRegularServer(1);

            InitializeClient();

            ProcessRequest();
        }

        [Test]
        public void Client_should_return_success_when_all_replicas_are_good()
        {
            StartRegularServer(3);

            InitializeClient();

            ProcessRequest();
        }

        [Test]
        public void Client_should_return_success_when_one_replica_is_good_and_others_are_bad()
        {
            StartRegularServer(1);
            StartSlowServer(2);

            InitializeClient();

            ProcessRequest();
        }

        [Test]
        public void Client_should_fail_when_all_replicas_are_bad()
        {
            StartSlowServer(3);

            InitializeClient();

            Action action = () => ProcessRequest();

            action.Should().Throw<TimeoutException>();
        }


        private void InitializeClient()
        {
            var replicaAddresses = GetAddress().ToArray();
            switch (clientName)
            {
                case "random":
                    client = new RandomClusterClient(replicaAddresses);
                    break;
                case "simultaneous":
                    client = new SimultaneousClusterClient(replicaAddresses);
                    break;
                case "round_robin":
                    client = new RoundRobinClusterClient(replicaAddresses);
                    break;
                case "smart":
                    client = new SmartClusterClient(replicaAddresses);
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        private IEnumerable<string> GetAddress()
        {
            foreach (var clusterServer in clusterServers)
                yield return $"http://localhost:{clusterServer.ServerOptions.Port}/{clusterServer.ServerOptions.MethodName}/";
        }

        private void StartRegularServer(int count = 1)
        {
            for (var i = 0; i < count; i++)
            {
                var serverOptions = new ServerOptions { Async = true, MethodDuration = 10, MethodName = "some_method", Port = GetFreePort() };
                var server = new ClusterServer(serverOptions, log);
                clusterServers.Add(server);

                Console.WriteLine($"Started server at port {serverOptions.Port}");

                server.Start();
            }
        }

        private void StartSlowServer(int count = 1)
        {
            for (var i = 0; i < count; i++)
            {
                var serverOptions = new ServerOptions { Async = true, MethodDuration = 1000000, MethodName = "some_method", Port = GetFreePort() };
                var server = new ClusterServer(serverOptions, log);
                clusterServers.Add(server);

                server.Start();
            }
        }

        private string[] ProcessRequest()
        {
            var queries = new[]
            {
                "lorem", "ipsum", "dolor", "sit", "amet", "consectetuer",
                "adipiscing", "elit", "sed", "diam", "nonummy", "nibh", "euismod",
                "tincidunt", "ut", "laoreet", "dolore", "magna", "aliquam", "erat"
            };

            Console.WriteLine("Testing {0} started", client.GetType());
            var result = Task.WhenAll(queries.Select(
                async query =>
                {
                    var timer = Stopwatch.StartNew();
                    try
                    {
                        var clientResult = await client.ProcessRequestAsync(query, TimeSpan.FromSeconds(6));

                        clientResult.Should().Be(Encoding.UTF8.GetString(ClusterHelpers.GetBase64HashBytes(query)));

                        Console.WriteLine("Query \"{0}\" successful ({1} ms)", query, timer.ElapsedMilliseconds);

                        return clientResult;
                    }
                    catch (TimeoutException)
                    {
                        Console.WriteLine($"Query \"{query}\" timeout ({timer.ElapsedMilliseconds} ms) {DateTime.Now.TimeOfDay}");
                        throw;
                    }
                }).ToArray()).GetAwaiter().GetResult();
            Console.WriteLine("Testing {0} finished", client.GetType());

            return result;
        }


        private void StopServers()
        {
            foreach (var clusterServer in clusterServers)
                clusterServer.Stop();
        }

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            try
            {
                listener.Start();
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }

        private List<ClusterServer> clusterServers;
        private ILog log;
        private ClusterClientBase client;
    }
}
