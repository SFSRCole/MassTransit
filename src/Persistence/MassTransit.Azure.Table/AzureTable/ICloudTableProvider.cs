namespace MassTransit.AzureTable
{
    using Azure.Data.Tables;


    public interface ITableClientProvider<in TSaga>
        where TSaga : class, ISaga
    {
        TableClient GetTableClient();
    }
}
