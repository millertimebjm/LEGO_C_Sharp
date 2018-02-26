using LegoLoad.Models;
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
            var parts = GetParts();
            DriverLoadParts(parts);
            var sets = GetSets();
            DriverLoadSets(sets);
            var inventories = GetInventories();
            var inventoryParts = GetInventoryParts();

            
        }

        private static IEnumerable<Inventory> GetInventories()
        {
            var inventories = ReadCsv.Process(@"C:\temp\BigData\LEGO\inventories.csv")
                .Skip(1)
                .Select(_ => new Inventory()
                {
                    Id = _[0],
                    SetId = _[1],
                });
            return inventories;
        }

        private static IEnumerable<InventoryPart> GetInventoryParts()
        {
            var inventoryParts = ReadCsv.Process(@"C:\temp\BigData\LEGO\inventory_parts.csv")
                .Skip(1)
                .Select(_ => new InventoryPart()
                {
                    InventoryId = _[0].As<int>(),
                    PartId = _[1],
                    Quantity = _[2].As<int>(),
                });
            return inventoryParts;
        }

        private static IEnumerable<Part> GetParts()
        {
            var parts = ReadCsv.Process(@"C:\temp\BigData\LEGO\parts.csv")
                .Skip(1)
                .Select(_ => new Part()
                {
                    Id = _[0],
                    Description = _[1],
                });
            return parts;
        }

        private static IEnumerable<Set> GetSets()
        {
            var sets = ReadCsv.Process(@"C:\temp\BigData\LEGO\sets.csv")
                .Skip(1)
                .Select(_ => new Set()
                {
                    Id = _[0],
                    Name = _[1],
                    Year = _[2].As<int>(),
                });
            return sets;
        }

        private static void DriverLoadParts(IEnumerable<Part> parts)
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Part) DELETE a");

                foreach (var part in parts)
                {
                    var insertResult = driver.InsertPart(part);
                }
                // 47 seconds
            }
        }

        private static void DriverLoadSets(IEnumerable<Set> sets)
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Set) DELETE a");

                foreach (var set in sets)
                {
                    var insertResult = driver.InsertSet(set);
                }
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
