namespace MassTransit.AzureTable.Saga
{
    using System;
    using Azure.Data.Tables;


    public class DelegateTableClientProvider<TSaga> :
        ITableClientProvider<TSaga>
        where TSaga : class, ISaga
    {
        readonly Func<TableClient> _TableClient;

        public DelegateTableClientProvider(Func<TableClient> TableClient)
        {
            _TableClient = TableClient;
        }

        public TableClient GetTableClient()
        {
            return _TableClient();
        }
    }
}
