using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BenchmarkDatabase
{
    public class Global
    {
        public const string PostgreSQL = "User ID=postgres;Password=root;Host=localhost;Port=5432;Database=benchmark;";
        public const string MSSQL = "Server=.\\SQLEXPRESS;Initial Catalog=benchmark;Integrated Security=True;TrustServerCertificate=true;";
        public const string MongoDB = "mongodb://localhost:27017/benchmark";

        private static IMongoCollection<TestModelMongo>? _data { get; set; }
        public static IMongoCollection<TestModelMongo>? GetMongoTestData()
        {
            if (_data is null)
            {
                var connection = new MongoUrlBuilder(MongoDB);
                MongoClient client = new MongoClient(MongoDB);
                IMongoDatabase database = client.GetDatabase(connection.DatabaseName);
                _data = database.GetCollection<TestModelMongo>("test_data"); 
            }

            return _data;
        }
    }
}
