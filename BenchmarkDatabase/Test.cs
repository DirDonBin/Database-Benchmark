using BenchmarkDatabase.Benchmarks;
using BenchmarkDotNet.Running;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BenchmarkDatabase
{
    [TestClass]
    public class Test
    {
        [TestMethod]
        public void TestDatabase()
        {
            BenchmarkRunner.Run<GetRecordsBenchmark>();
            BenchmarkRunner.Run<InsertBenchmark>();
            BenchmarkRunner.Run<InsertBulkBenchmark>();
            BenchmarkRunner.Run<UpdateBenchmark>();
            BenchmarkRunner.Run<UpdateBulkBenchmark>();
            BenchmarkRunner.Run<DeleteBenchmark>();
            BenchmarkRunner.Run<DeleteBulkBenchmark>();
        }
    }
}
