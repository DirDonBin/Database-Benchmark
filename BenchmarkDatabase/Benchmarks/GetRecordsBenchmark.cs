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
    [HtmlExporter()]
    [MarkdownExporterAttribute.GitHub()]
    public class GetRecordsBenchmark
    {

        [Params(1, 10, 100, 1000, 10000, 100000)]
        public int N = 10;

        [Benchmark(Description = "MsSQl - get random record")]
        public void MSSQL()
        {
            using (IDbConnection db = new SqlConnection(Global.MSSQL))
            {
                var pr = db.QueryFirst<(int min, int max)>("select min = Min(Id), max = Max(Id) from test_data");
                var id = Random.Shared.Next(pr.min, pr.max - 200000);
                var tmp = db.Query<TesstModel>($"SELECT Top {N} * FROM [dbo].[test_data] where Id > {id}").ToList();
            }
        }

        [Benchmark(Description = "PotgreSQL - get random record")]
        public void PostgreSQL()
        {
            using (IDbConnection db = new NpgsqlConnection(Global.PostgreSQL))
            {
                var pr = db.QueryFirst<(int min, int max)>("Select Min(\"Id\"), Max(\"Id\") From test_data");
                var id = Random.Shared.Next(pr.min, pr.max - 200000);
                var tmp = db.Query<TesstModel>($"SELECT * FROM public.test_data where \"Id\" > {id} LIMIT {N}").ToList();
            }
        }

        [Benchmark(Description = "MongoDB - get random record")]
        public void MongoDB()
        {
            var data = Global.GetMongoTestData();

            var tmp = data.AsQueryable().Sample(N).ToList();
        }
    }
}
