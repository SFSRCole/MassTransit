namespace MassTransit.AzureTable.Saga
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using MassTransit.Saga;
    using Azure.Data.Tables;


    public class AzureTableSagaRepositoryContextFactory<TSaga> :
        ISagaRepositoryContextFactory<TSaga>,
        ILoadSagaRepositoryContextFactory<TSaga>
        where TSaga : class, ISaga
    {
        readonly ITableClientProvider<TSaga> _TableClientProvider;
        readonly ISagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga> _factory;
        readonly ISagaKeyFormatter<TSaga> _keyFormatter;

        public AzureTableSagaRepositoryContextFactory(ITableClientProvider<TSaga> TableClientProvider,
            ISagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga> factory,
            ISagaKeyFormatter<TSaga> keyFormatter)
        {
            _TableClientProvider = TableClientProvider;
            _factory = factory;
            _keyFormatter = keyFormatter;
        }

        public AzureTableSagaRepositoryContextFactory(TableClient TableClient,
            ISagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga> factory,
            ISagaKeyFormatter<TSaga> keyFormatter)
            : this(new ConstTableClientProvider<TSaga>(TableClient), factory, keyFormatter)
        {
        }

        public Task<T> Execute<T>(Func<LoadSagaRepositoryContext<TSaga>, Task<T>> asyncMethod, CancellationToken cancellationToken = default)
            where T : class
        {
            var database = _TableClientProvider.GetTableClient();

            var databaseContext = new AzureTableDatabaseContext<TSaga>(database, _keyFormatter);
            var repositoryContext = new CosmosTableSagaRepositoryContext<TSaga>(databaseContext, cancellationToken);

            return asyncMethod(repositoryContext);
        }

        public void Probe(ProbeContext context)
        {
            context.Add("persistence", "azuretable");
        }

        public async Task Send<T>(ConsumeContext<T> context, IPipe<SagaRepositoryContext<TSaga, T>> next)
            where T : class
        {
            var database = _TableClientProvider.GetTableClient();

            var databaseContext = new AzureTableDatabaseContext<TSaga>(database, _keyFormatter);

            var repositoryContext = new AzureTableSagaRepositoryContext<TSaga, T>(databaseContext, context, _factory);

            await next.Send(repositoryContext).ConfigureAwait(false);
        }

        public async Task SendQuery<T>(ConsumeContext<T> context, ISagaQuery<TSaga> query, IPipe<SagaRepositoryQueryContext<TSaga, T>> next)
            where T : class
        {
            throw new NotImplementedByDesignException("Azure Table repository does not support queries");
        }
    }
}
