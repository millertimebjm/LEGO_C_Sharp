using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad
{
    class Program
    {
        /// <summary>
        /// Cypher formatting 
        ///     Match ([alias]:[Table])
        ///     RETURN [alias]
        ///     
        ///     Match ([alias]:[Table])
        ///     RETURN [alias].[Column]
        ///     
        ///     CREATE ([alias]:[Table] {[column1]: '[value1]', [column2]: '[column2]', ... })
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            //DriverAdapter();
            //ClientAdapter();
            var parts = DriverLoadParts();
            var sets = DriverLoadSets();
            
            

            
        }

        private static List<Part> DriverLoadParts()
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Part) DELETE a");

                var partsCsv = ReadCsv.Process(@"C:\temp\BigData\LEGO\parts.csv").Skip(1);
                var parts = new List<Part>();

                foreach (var partArray in partsCsv)
                {
                    var part = new Part()
                    {
                        Id = partArray[0],
                        Description = partArray[1],
                    };
                    parts.Add(part);
                    var insertResult = driver.InsertPart(part);
                }
                // 47 seconds
                return parts;
            }
        }

        private static List<Set> DriverLoadSets()
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Set) DELETE a");

                var setsCsv = ReadCsv.Process(@"C:\temp\BigData\LEGO\sets.csv").Skip(1);
                var sets = new List<Set>();
                foreach (var setArray in sets)
                {
                    var set = new Set()
                    {
                        Id = setArray[0],
                        Name = setArray[1],
                        Year = setArray[2].As<int>(),
                    };
                    sets.Add(set);
                    var insertResult = driver.InsertSet(set);
                }
                return sets;
            }
        }

        private static void DriverAdapter()
        {
            using (var greeter = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                //var greeting = greeter.CreateGreeting("hello, world");
                //Console.WriteLine($"{greeting.Message} in {greeting.ExecutionTimeInMilliseconds} milliseconds.");
                var deleteResult = greeter.ExecuteCypher("MATCH (a:Part) DELETE a");

                //var part = new Part()
                //{
                //    Id = "10039",
                //    Description = "Pullback Motor 8 x 4 x 2/3",
                //};
                //var insertResult = greeter.InsertPart(part);
                //var getResult = ((Part)greeter.GetNode(part.Id).Data);
                //Console.WriteLine(getResult.Description);
            }
        }

        /// <summary>
        /// var query = client
        ///     .Cypher
        ///     .Start(new { root = client.RootNode })
        ///     .Match("root-[:HAS_BOOK]->book")
        ///     .Where((Book bk) => bk.Pages > 5)
        ///     .Return<Book>("book");
        /// var longBooks = query.Results;
        /// 
        /// match (n:Book) return n
        /// </summary>
        private static void ClientAdapter()
        {
            var client = new ClientAdapter("http://localhost:7687/db/data", "neo4j", "krampus");

            
        }
    }
}
