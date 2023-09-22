namespace MassTransit.AzureTable.Saga
{
    using Azure.Data.Tables;


    public class ConstTableClientProvider<TSaga> :
        ITableClientProvider<TSaga>
        where TSaga : class, ISaga
    {
        readonly TableClient _TableClient;

        public ConstTableClientProvider(TableClient TableClient)
        {
            _TableClient = TableClient;
        }

        public TableClient GetTableClient()
        {
            return _TableClient;
        }
    }
}
