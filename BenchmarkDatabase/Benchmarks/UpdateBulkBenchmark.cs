using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.SqlClient;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
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
    public class UpdateBulkBenchmark
    {
        [Params(1, 10, 100, 1000, 10000, 100000)]
        public int N = 10000;

        [Benchmark(Description = "MsSQl - random bulk update")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var pr = db.QueryFirst<(int min, int max)>("select min = Min(Id), max = Max(Id) from test_data");
                var id = Random.Shared.Next(pr.min, pr.max - 20000);
                var records = db.Query<TesstModel>($"Select Top {N} * From test_data Where Id > {id}").ToList();

                mdls.ForEach(x => x.Id = records[mdls.IndexOf(x)].Id);

                var queries = GetMSSQLQuery(mdls);

                db.Execute(queries);
            }
        }

        [Benchmark(Description = "PostgreSQL - random bulk update")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var pr = db.QueryFirst<(int min, int max)>("Select Min(\"Id\"), Max(\"Id\") From test_data");
                var id = Random.Shared.Next(pr.min, pr.max - 20000);
                var records = db.Query<TesstModel>($"Select * From test_data Where \"Id\" > {id} Limit {N}").ToList();

                mdls.ForEach(x => x.Id = records[mdls.IndexOf(x)].Id);

                var queries = GetPostgreSQLQuery(mdls);

                db.Execute(queries);
            }
        }

        [Benchmark(Description = "MongoDB - random bulk update")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();
            var mdls = BaseTestModel.GetList<TestModelMongo>(N);
            var records = data.AsQueryable().Sample(N).ToList();

            var listWrites = new List<WriteModel<TestModelMongo>>();

            mdls.ForEach(x =>
            {
                x.Id = records[mdls.IndexOf(x)].Id;

                var filter = Builders<TestModelMongo>.Filter.Eq(p => p.Id, x.Id);
                var update = Builders<TestModelMongo>.Update
                    .Set(x => x.Region, x.Region)
                    .Set(x => x.Country, x.Country)
                    .Set(x => x.ItemType, x.ItemType)
                    .Set(x => x.OrderDate, x.OrderDate)
                    .Set(x => x.OrderID, x.OrderID)
                    .Set(x => x.OrderPriority, x.OrderPriority)
                    .Set(x => x.SalesChannel, x.SalesChannel)
                    .Set(x => x.ShipDate, x.ShipDate)
                    .Set(x => x.TotalCost, x.TotalCost)
                    .Set(x => x.TotalProfit, x.TotalProfit)
                    .Set(x => x.TotalRevenue, x.TotalRevenue)
                    .Set(x => x.UnitCost, x.UnitCost)
                    .Set(x => x.UnitPrice, x.UnitPrice)
                    .Set(x => x.UnitsSold, x.UnitsSold)
                    ;

                listWrites.Add(new UpdateOneModel<TestModelMongo>(filter, update));
            });

            data!.BulkWrite(listWrites);
        }




        private string GetMSSQLQuery(List<TesstModel> mdls)
        {
            var data = mdls.Select(x => @$"
                                update test_data
                                   set [Region]    = '{x.Region}',
                                   [Country]       = '{x.Country}',
                                   [ItemType]      = '{x.ItemType}',
                                   [SalesChannel]  = '{x.SalesChannel}',
                                   [OrderPriority] = '{x.OrderPriority}',
                                   [OrderDate]     = '{x.OrderDate}',
                                   [OrderID]       =  {x.OrderID},
                                   [ShipDate]      = '{x.ShipDate}',
                                   [UnitsSold]     =  {x.UnitsSold},
                                   [UnitPrice]     = '{x.UnitPrice}',
                                   [UnitCost]      = '{x.UnitCost}',
                                   [TotalRevenue]  = '{x.TotalRevenue}',
                                   [TotalCost]     = '{x.TotalCost}',
                                   [TotalProfit]  = '{x.TotalProfit}'
                                   Where Id = {x.Id}"
                                ).ToList();

            var query = string.Join("\r\n", data);

            return query;
        }

        private string GetPostgreSQLQuery(List<TesstModel> mdls)
        {
            var data = mdls.Select(x => @$"
                                update test_data
                                   set ""Region""    = '{x.Region}',
                                   ""Country""       = '{x.Country}',
                                   ""ItemType""      = '{x.ItemType}',
                                   ""SalesChannel""  = '{x.SalesChannel}',
                                   ""OrderPriority"" = '{x.OrderPriority}',
                                   ""OrderDate""     = '{x.OrderDate}',
                                   ""OrderID""       =  {x.OrderID},
                                   ""ShipDate""      = '{x.ShipDate}',
                                   ""UnitsSold""     =  {x.UnitsSold},
                                   ""UnitPrice""     = '{x.UnitPrice}',
                                   ""UnitCost""      = '{x.UnitCost}',
                                   ""TotalRevenue""  = '{x.TotalRevenue}',
                                   ""TotalCost""     = '{x.TotalCost}',
                                   ""TotalProfit""  = '{x.TotalProfit}'
                                   Where ""Id"" = {x.Id};"
                                ).ToList();

            var query = string.Join("\r\n", data);

            return query;
        }
    }
}
