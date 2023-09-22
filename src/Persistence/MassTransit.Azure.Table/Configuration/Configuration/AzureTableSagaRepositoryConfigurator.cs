namespace MassTransit.Configuration
{
    using System;
    using System.Collections.Generic;
    using AzureTable;
    using AzureTable.Saga;
    using Azure.Data.Tables;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Saga;


    public class AzureTableSagaRepositoryConfigurator<TSaga> :
        IAzureTableSagaRepositoryConfigurator<TSaga>,
        ISpecification
        where TSaga : class, ISaga
    {
        Func<IServiceProvider, TableClient> _connectionFactory;

        Func<IServiceProvider, ISagaKeyFormatter<TSaga>> _formatterFactory = provider =>
            new ConstPartitionSagaKeyFormatter<TSaga>(typeof(TSaga).Name);

        /// <summary>
        /// Supply factory for retrieving the Table Client.
        /// </summary>
        /// <param name="connectionFactory"></param>
        public void ConnectionFactory(Func<TableClient> connectionFactory)
        {
            _connectionFactory = provider => connectionFactory();
        }

        /// <summary>
        /// Supply factory for retrieving the Table Client.
        /// </summary>
        /// <param name="connectionFactory"></param>
        public void ConnectionFactory(Func<IServiceProvider, TableClient> connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        /// <summary>
        /// Supply factory for retrieving the key formatter.
        /// </summary>
        /// <param name="formatterFactory"></param>
        public void KeyFormatter(Func<ISagaKeyFormatter<TSaga>> formatterFactory)
        {
            _formatterFactory = provider => formatterFactory();
        }

        public IEnumerable<ValidationResult> Validate()
        {
            if (_connectionFactory == null)
                yield return this.Failure("ConnectionFactory", "must be specified");
        }

        public void Register<T>(ISagaRepositoryRegistrationConfigurator<T> configurator)
            where T : class, ISaga
        {
            configurator.TryAddSingleton<ITableClientProvider<TSaga>>(provider => new ConstTableClientProvider<TSaga>(_connectionFactory(provider)));
            configurator.TryAddSingleton(_formatterFactory);
            configurator.RegisterLoadSagaRepository<T, AzureTableSagaRepositoryContextFactory<T>>();
            configurator
                .RegisterSagaRepository<T, DatabaseContext<T>, SagaConsumeContextFactory<DatabaseContext<T>, T>, AzureTableSagaRepositoryContextFactory<T>>();
        }
    }
}
