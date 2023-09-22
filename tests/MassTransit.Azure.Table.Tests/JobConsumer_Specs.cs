namespace MassTransit.Azure.Table.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using Contracts.JobService;
    using JobConsumerTests;
    using global::Azure.Data.Tables;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;
    using Testing;
    using MassTransit.Internals;

    namespace JobConsumerTests
    {
        using System;
        using System.Threading.Tasks;
        using Contracts.JobService;


        public interface OddJob
        {
            TimeSpan Duration { get; }
        }


        public class OddJobConsumer :
            IJobConsumer<OddJob>
        {
            public async Task Run(JobContext<OddJob> context)
            {
                if (context.RetryAttempt == 0)
                    await Task.Delay(context.Job.Duration, context.CancellationToken);
            }
        }


        public class OddJobCompletedConsumer :
            IConsumer<JobCompleted<OddJob>>
        {
            public Task Consume(ConsumeContext<JobCompleted<OddJob>> context)
            {
                return Task.CompletedTask;
            }
        }
    }


    public class Using_the_new_job_service_configuration
    {
        const string TableName = "jobservice";

        [Test]
        public async Task Should_complete_the_job()
        {
            await using var provider = new ServiceCollection()
                .AddSingleton(provider =>
                {
                    var connectionString = Configuration.StorageAccount;
                    var tableServiceClient = new TableServiceClient(connectionString);

                    return tableServiceClient;
                })
                .AddHostedService<CreateTableHostedService>()
                .AddMassTransitTestHarness(x =>
                {
                    x.SetTestTimeouts(testInactivityTimeout: TimeSpan.FromSeconds(10));

                    x.SetKebabCaseEndpointNameFormatter();

                    x.AddConsumer<OddJobConsumer>()
                        .Endpoint(e => e.Name = "odd-job");

                    x.AddConsumer<OddJobCompletedConsumer>()
                        .Endpoint(e => e.ConcurrentMessageLimit = 1);

                    x.SetJobConsumerOptions(options => options.HeartbeatInterval = TimeSpan.FromSeconds(10))
                        .Endpoint(e => e.PrefetchCount = 100);

                    x.AddJobSagaStateMachines()
                        .AzureTableRepository(r =>
                        {
                            r.ConnectionFactory(provider => provider.GetRequiredService<TableServiceClient>().GetTableClient(TableName));
                        });

                    x.UsingInMemory((context, cfg) =>
                    {
                        cfg.UseDelayedMessageScheduler();

                        cfg.ConfigureEndpoints(context);
                    });
                })
                .BuildServiceProvider(true);

            var harness = await provider.StartTestHarness();
            try
            {
                var jobId = NewId.NextGuid();

                IRequestClient<SubmitJob<OddJob>> client = harness.GetRequestClient<SubmitJob<OddJob>>();

                Response<JobSubmissionAccepted> response = await client.GetResponse<JobSubmissionAccepted>(new
                {
                    JobId = jobId,
                    Job = new { Duration = TimeSpan.FromSeconds(1) }
                });

                Assert.That(response.Message.JobId, Is.EqualTo(jobId));

                Assert.That(await harness.Published.Any<JobSubmitted>(), Is.True);
                Assert.That(await harness.Published.Any<JobStarted>(), Is.True);

                Assert.That(await harness.Published.Any<JobCompleted>(), Is.True);
                Assert.That(await harness.Published.Any<JobCompleted<OddJob>>(), Is.True);
            }
            finally
            {
                await harness.Stop();
            }
        }

        [OneTimeSetUp]
        public async Task Setup()
        {
        }

        [OneTimeTearDown]
        public async Task Teardown()
        {
        }


        public class CreateTableHostedService :
            IHostedService
        {
            readonly ILogger<CreateTableHostedService> _logger;
            readonly IServiceProvider _provider;

            public CreateTableHostedService(IServiceProvider provider, ILogger<CreateTableHostedService> logger)
            {
                _provider = provider;
                _logger = logger;
            }

            public async Task StartAsync(CancellationToken cancellationToken)
            {
                _logger.LogInformation("Creating table configuration in Azure Table Storage");

                var table = _provider.GetRequiredService<TableServiceClient>().GetTableClient(TableName);

                await table.CreateIfNotExistsAsync(cancellationToken);

                var results = await table.QueryAsync<TableEntity>(x => true, null, null, cancellationToken).ToListAsync();

                var batchDeleteOperation = new List<TableTransactionAction>();

                foreach (var row in results)
                    batchDeleteOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, row));

                await table.SubmitTransactionAsync(batchDeleteOperation, cancellationToken);
            }

            public async Task StopAsync(CancellationToken cancellationToken)
            {
            }
        }
    }
}
