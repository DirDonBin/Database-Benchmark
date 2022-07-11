using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BenchmarkDatabase.Benchmarks
{
    [MinColumn, MaxColumn, MeanColumn, MedianColumn]
    [RankColumn]
    [HtmlExporter]
    public class InsertBenchmark
    {
        [Params(1, 10, 100, 1000, 10000, 100000)]
        public int N = 10;

        [Benchmark(Description = "MsSQl - random insert")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var queries = GetMSSQLInsertQuery(mdls);

                foreach (var query in queries)
                {
                    db.Execute(query);
                }
            }
        }

        [Benchmark(Description = "PostgreSQL - random insert")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var queries = GetPostgreSQLInsertQuery(mdls);

                foreach (var query in queries)
                {
                    db.Execute(query);
                }
            }
        }

        [Benchmark(Description = "MongoDB - random insert")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();
            var mdls = BaseTestModel.GetList<TestModelMongo>(N);

            data!.InsertMany(mdls);
        }






        private List<string> GetMSSQLInsertQuery(List<TesstModel> mdls)
        {
            var queries = mdls.Select(x => $@"
                                insert into test_data(
                                   [Region]
                                  ,[Country]
                                  ,[ItemType]
                                  ,[SalesChannel]
                                  ,[OrderPriority]
                                  ,[OrderDate]
                                  ,[OrderID]
                                  ,[ShipDate]
                                  ,[UnitsSold]
                                  ,[UnitPrice]
                                  ,[UnitCost]
                                  ,[TotalRevenue]
                                  ,[TotalCost]
                                  ,[TotalProfit])
                                Values
                                ('{x.Region}',
                                '{x.Country}',
                                '{x.ItemType}',
                                '{x.SalesChannel}',
                                '{x.OrderPriority}',
                                '{x.OrderDate}',
                                 {x.OrderID},
                                '{x.ShipDate}',
                                 {x.UnitsSold},
                                '{x.UnitPrice}',
                                '{x.UnitCost}',
                                '{x.TotalRevenue}',
                                '{x.TotalCost}',
                                '{x.TotalProfit}')

                                ").ToList();

            return queries;
        }

        private List<string> GetPostgreSQLInsertQuery(List<TesstModel> mdls)
        {
            var queries = mdls.Select(x => $@"
                                insert into test_data(
                                   ""Region""
                                  ,""Country""
                                  ,""ItemType""
                                  ,""SalesChannel""
                                  ,""OrderPriority""
                                  ,""OrderDate""
                                  ,""OrderID""
                                  ,""ShipDate""
                                  ,""UnitsSold""
                                  ,""UnitPrice""
                                  ,""UnitCost""
                                  ,""TotalRevenue""
                                  ,""TotalCost""
                                  ,""TotalProfit"")
                                Values
                                ('{x.Region}',
                                '{x.Country}',
                                '{x.ItemType}',
                                '{x.SalesChannel}',
                                '{x.OrderPriority}',
                                '{x.OrderDate}',
                                 {x.OrderID},
                                '{x.ShipDate}',
                                 {x.UnitsSold},
                                '{x.UnitPrice}',
                                '{x.UnitCost}',
                                '{x.TotalRevenue}',
                                '{x.TotalCost}',
                                '{x.TotalProfit}')

                                ").ToList();

            return queries;
        }

    }
}
