namespace MassTransit.Azure.Table.Tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using global::Azure.Data.Tables;
    using NUnit.Framework;
    using TestFramework;


    public class AzureTableInMemoryTestFixture :
        InMemoryTestFixture
    {
        protected readonly string ConnectionString;
        protected readonly TableClient TestTableClient;
        protected readonly string TestTableName;

        public AzureTableInMemoryTestFixture()
        {
            ConnectionString = Configuration.StorageAccount;
            TestTableName = "azuretabletests";
            var tableServiceClient = new TableServiceClient(ConnectionString);
            TestTableClient = tableServiceClient.GetTableClient(TestTableName);
        }
        
        protected override void ConfigureInMemoryBus(IInMemoryBusFactoryConfigurator configurator)
        {
            configurator.UseDelayedMessageScheduler();

            base.ConfigureInMemoryBus(configurator);
        }

        public IEnumerable<T> GetRecords<T>() where T : class, ITableEntity 
        {
            return TestTableClient.Query<T>(x=>true).ToList();
        }

        public IEnumerable<TableEntity> GetTableEntities()
        {
            return TestTableClient.Query<TableEntity>(x => true).ToList();
        }

        [OneTimeSetUp]
        public async Task Bring_it_up()
        {
            TestTableClient.CreateIfNotExists();

            IEnumerable<TableEntity> entities = GetTableEntities();

            // Create the batch operation.
            var batchDeleteOperation = new List<TableTransactionAction>();

            foreach (var row in entities)
                batchDeleteOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, row));

            await TestTableClient.SubmitTransactionAsync(batchDeleteOperation);
        }
    }
}
