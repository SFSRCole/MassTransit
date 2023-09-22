namespace MassTransit.AzureTable.Saga
{
    using System;
    using MassTransit.Saga;
    using Azure.Data.Tables;


    public static class AzureTableSagaRepository<TSaga>
        where TSaga : class, ISaga
    {
        public static ISagaRepository<TSaga> Create(Func<TableClient> tableFactory, ISagaKeyFormatter<TSaga> keyFormatter)
        {
            var consumeContextFactory = new SagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga>();

            var TableClientProvider = new DelegateTableClientProvider<TSaga>(tableFactory);

            var repositoryContextFactory = new AzureTableSagaRepositoryContextFactory<TSaga>(TableClientProvider, consumeContextFactory, keyFormatter);

            return new SagaRepository<TSaga>(repositoryContextFactory, loadSagaRepositoryContextFactory: repositoryContextFactory);
        }

        public static ISagaRepository<TSaga> Create(Func<TableClient> tableFactory)
        {
            return Create(tableFactory, new ConstPartitionSagaKeyFormatter<TSaga>(typeof(TSaga).Name));
        }
    }
}
