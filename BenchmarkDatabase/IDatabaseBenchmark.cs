namespace BenchmarkDatabase
{
    public interface IDatabaseBenchmark
    {
        void GetRecords();

        void InsertRecords();

        void UpdateRecords();

        void DeleteRecords();

        void InsertBulkRecords();
                   
        void UpdateBulkRecords();
                   
        void DeleteBulkRecords();
    }

    
}
