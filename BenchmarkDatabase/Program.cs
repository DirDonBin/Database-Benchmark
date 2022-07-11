
using BenchmarkDatabase;
using BenchmarkDatabase.Benchmarks;
using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.SqlClient;
using System.Data;

Console.WriteLine("Start benchmark");


#if DEBUG 

new GetRecordsBenchmark().MSSQL();

#else

var tests = new Test();
tests.TestDatabase();

#endif






Console.ReadKey();














