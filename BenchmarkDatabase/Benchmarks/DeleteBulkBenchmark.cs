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
    public class DeleteBulkBenchmark
    {
        [Params(1, 10, 100, 1000, 10000)]
        public int N = 10;

        [Benchmark(Description = "MsSQl - random bulk delete")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var records = db.Query<TesstModel>($"Select Top {N} * From test_data Order by Id desc").ToList();

                var query = $"Delete test_data where Id in ({string.Join(",", records.Select(x => x.Id))})";

                db.Execute(query);
            }
        }

        [Benchmark(Description = "PostgreSQL - random bulk delete")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var records = db.Query<TesstModel>($"Select * From test_data ORDER BY \"Id\" DESC Limit {N}").ToList();

                var query = $"Delete from test_data where \"Id\" in ({string.Join(",", records.Select(x => x.Id))})";

                db.Execute(query);
            }
        }

        [Benchmark(Description = "MongoDB - random bulk delete")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();
            var records = data.AsQueryable().Sample(N).ToList().Select(x => new DeleteOneModel<TestModelMongo>(Builders<TestModelMongo>.Filter.Eq(p => p.Id, x.Id))).ToList();

            var listWrites = new List<WriteModel<TestModelMongo>>();

            listWrites.AddRange(records);

            data!.BulkWrite(listWrites);
        }


    }
}
