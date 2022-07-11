using Bogus;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BenchmarkDatabase
{
    public abstract class BaseTestModel
    {
        public string Region { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
        public string ItemType { get; set; } = string.Empty;
        public string SalesChannel { get; set; } = string.Empty;
        public string OrderPriority { get; set; } = string.Empty;
        public string OrderDate { get; set; } = string.Empty;
        public int OrderID { get; set; }
        public string ShipDate { get; set; } = string.Empty;
        public short UnitsSold { get; set; }
        public string UnitPrice { get; set; } = string.Empty;
        public string UnitCost { get; set; } = string.Empty;
        public string TotalRevenue { get; set; } = string.Empty;
        public string TotalCost { get; set; } = string.Empty;
        public string TotalProfit { get; set; } = string.Empty;

        public static List<T> GetList<T>(int count) where T : BaseTestModel
        {
            return new Faker<T>()
                    .StrictMode(true)
                    .RuleFor(x => x.Region, (f, y) => string.Concat(f.Address.Direction().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.Country, (f, y) => string.Concat(f.Address.Country().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.ItemType, (f, y) => string.Concat(f.Commerce.Categories(1).First().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.SalesChannel, (f, y) => "Offline")
                    .RuleFor(x => x.OrderPriority, (f, y) => string.Concat(f.Address.City().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.OrderDate, (f, y) => f.Date.Recent().ToShortDateString())
                    .RuleFor(x => x.OrderID, (f, y) => f.Random.Int(1000000, 5000000))
                    .RuleFor(x => x.ShipDate, (f, y) => f.Date.Recent().ToShortDateString())
                    .RuleFor(x => x.UnitsSold, (f, y) => f.Random.Short(1000, 5000))
                    .RuleFor(x => x.UnitPrice, (f, y) => string.Concat(f.Commerce.Price().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.UnitCost, (f, y) => string.Concat(f.Commerce.Price().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.TotalRevenue, (f, y) => string.Concat(f.Commerce.Price().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.TotalCost, (f, y) => string.Concat(f.Commerce.Price().Take(500)).Replace("'", "''"))
                    .RuleFor(x => x.TotalProfit, (f, y) => string.Concat(f.Commerce.Price().Take(500)).Replace("'", "''"))
                    .Ignore("Id")
                    .Generate(count)
                    .ToList();
        }
    }

    public class TesstModel : BaseTestModel
    {
        public int Id { get; set; }
    }

    public class TestModelMongo : BaseTestModel
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = string.Empty;
    }
}
