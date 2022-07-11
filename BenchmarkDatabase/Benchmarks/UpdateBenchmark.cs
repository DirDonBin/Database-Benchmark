using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.SqlClient;
using MongoDB.Bson;
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
    public class UpdateBenchmark
    {
        [Params(1, 10, 100, 1000, 10000, 100000)]
        public int N = 10;

        [Benchmark(Description = "MsSQl - random update")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var queries = GetMSSQLQuery(mdls);

                foreach (var query in queries)
                {
                    var pr = db.QueryFirst<(int min, int max)>("select min = Min(Id), max = Max(Id) from test_data");
                    var id = Random.Shared.Next(pr.min, pr.max - 20000);
                    var record = db.Query<TesstModel>($"Select Top 1 * From test_data Where Id > {id}").First();
                    var qr = query.Replace($"__ID__", record.Id.ToString());
                    db.Execute(qr);
                }
            }
        }

        [Benchmark(Description = "PostgreSQL - random update")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var mdls = BaseTestModel.GetList<TesstModel>(N);

                var queries = GetPostrgreSQLQuery(mdls);

                var pr = db.QueryFirst<(int min, int max)>("Select Min(\"Id\"), Max(\"Id\") From test_data");

                foreach (var query in queries)
                {
                    var id = Random.Shared.Next(pr.min, pr.max - 20000);
                    var record = db.Query<TesstModel>($"Select * From test_data Where \"Id\" > {id} Limit 1").First();
                    var qr = query.Replace($"__ID__", record.Id.ToString());
                    db.Execute(qr);
                }
            }
        }

        [Benchmark(Description = "MongoDB - random update")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();
            var mdls = BaseTestModel.GetList<TestModelMongo>(N);
            var records = data.AsQueryable().Sample(N).ToList();

            mdls.ForEach(x => x.Id = records[mdls.IndexOf(x)].Id);

            foreach (var item in mdls)
            {
                var def = Builders<TestModelMongo>.Update
                    .Set(x => x.Region, item.Region)
                    .Set(x => x.Country, item.Country)
                    .Set(x => x.ItemType, item.ItemType)
                    .Set(x => x.OrderDate, item.OrderDate)
                    .Set(x => x.OrderID, item.OrderID)
                    .Set(x => x.OrderPriority, item.OrderPriority)
                    .Set(x => x.SalesChannel, item.SalesChannel)
                    .Set(x => x.ShipDate, item.ShipDate)
                    .Set(x => x.TotalCost, item.TotalCost)
                    .Set(x => x.TotalProfit, item.TotalProfit)
                    .Set(x => x.TotalRevenue, item.TotalRevenue)
                    .Set(x => x.UnitCost, item.UnitCost)
                    .Set(x => x.UnitPrice, item.UnitPrice)
                    .Set(x => x.UnitsSold, item.UnitsSold)
                    ;

                data!.UpdateOne(x => x.Id == item.Id, def);
            }
        }








        private List<string> GetMSSQLQuery(List<TesstModel> mdls)
        {
            var queries = mdls.Select(x => @$"
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
                                   Where Id = __ID__"
                                ).ToList();

            return queries;
        }

        private List<string> GetPostrgreSQLQuery(List<TesstModel> mdls)
        {
            var queries = mdls.Select(x => @$"
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
                                   Where ""Id"" = __ID__"
                                ).ToList();

            return queries;
        }
    }
}
