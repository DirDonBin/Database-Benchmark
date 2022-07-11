using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.SqlClient;
using MongoDB.Driver;
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
    public class InsertBulkBenchmark
    {
        [Params(1, 10, 100, 1000, 10000, 100000)]
        public int N = 10000;


        [Benchmark(Description = "MsSQl - random bulk insert")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var query = GetMSSQLBulkInsertQuery(mdls);

                db.Execute(query);
            }
        }

        [Benchmark(Description = "PostgreSQL - random bulk insert")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var query = GetPostgreSQLBulkInsertQuery(mdls);

                db.Execute(query);
            }
        }

        [Benchmark(Description = "MongoDB - random bulk insert")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();
            var mdls = BaseTestModel.GetList<TestModelMongo>(N).Select(x => new InsertOneModel<TestModelMongo>(x)).ToList();

            var listWrites = new List<WriteModel<TestModelMongo>>();

            listWrites.AddRange(mdls);

            data!.BulkWrite(listWrites);
        }


        private string GetMSSQLBulkInsertQuery(List<TesstModel> mdls)
        {

            var data = mdls.Chunk(999).Select(x => @"
                                insert test_data(
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
                                Values"
                                +
                                string.Join(",",
                                    x.Select(y => $@"
                                    ('{y.Region}',
                                     '{y.Country}',
                                     '{y.ItemType}',
                                     '{y.SalesChannel}',
                                     '{y.OrderPriority}',
                                     '{y.OrderDate}',
                                      {y.OrderID},
                                     '{y.ShipDate}',
                                      {y.UnitsSold},
                                     '{y.UnitPrice}',
                                     '{y.UnitCost}',
                                     '{y.TotalRevenue}',
                                     '{y.TotalCost}',
                                     '{y.TotalProfit}')
                                    "))).ToList();

            var query = string.Join("\r\n", data);

            return query;
        }


        private string GetPostgreSQLBulkInsertQuery(List<TesstModel> mdls)
        {
            var data = mdls.Chunk(999).Select(x => @"
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
                                Values"
                                +
                                string.Join(",",
                                    x.Select(y => $@"
                                    ('{y.Region}',
                                     '{y.Country}',
                                     '{y.ItemType}',
                                     '{y.SalesChannel}',
                                     '{y.OrderPriority}',
                                     '{y.OrderDate}',
                                      {y.OrderID},
                                     '{y.ShipDate}',
                                      {y.UnitsSold},
                                     '{y.UnitPrice}',
                                     '{y.UnitCost}',
                                     '{y.TotalRevenue}',
                                     '{y.TotalCost}',
                                     '{y.TotalProfit}')
                                    ")) + ";").ToList();

            var query = string.Join("\r\n", data);

            return query;
        }
    }
}
