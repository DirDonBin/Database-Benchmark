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
    public class DeleteBenchmark
    {
        [Params(1, 10, 100, 1000, 10000)]
        public int N = 10;

        [Benchmark(Description = "MsSQl - random delete")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var records = db.Query<TesstModel>($"Select Top {N} * From test_data Order by Id desc").ToList();

                var queries = records.Select(x => $"Delete test_data where Id = {x.Id}").ToList();

                foreach(var query in queries)
                {
                    db.Execute(query);
                }
            }
        }

        [Benchmark(Description = "PostgreSQL - random delete")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                if (db.State == ConnectionState.Closed) db.Open();

                var records = db.Query<TesstModel>($"Select * From test_data ORDER BY \"Id\" DESC Limit 1").ToList();

                var queries = records.Select(x => $"Delete from test_data where \"Id\" = {x.Id}").ToList();

                foreach (var query in queries)
                {
                    db.Execute(query);
                }
            }
        }

        [Benchmark(Description = "MongoDB - random delete")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();
            var records = data.AsQueryable().Sample(N).ToList().Select(x => Builders<TestModelMongo>.Filter.Eq(p => p.Id, x.Id)).ToList();

            foreach (var item in records)
            {
                data!.DeleteOne(item);
            }
        }

    }
}
