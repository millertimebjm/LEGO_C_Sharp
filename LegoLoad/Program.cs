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
            var sets = GetSets();
            var inventories = GetInventories();
            var inventoryParts = GetInventoryParts();
            DriverLoadParts(parts);
            DriverLoadSets(sets);
            DriverLoadInventory(inventories);
            //DriverSetInventoryRelationship(sets, inventories, "CONTAINS", "CONTAINED_IN");
            //DriverInventoryPartRelationship(inventoryParts, parts, "CONTAINS", "CONTAINED_IN");
            DriverSetInventoryRelationship(sets, inventories, "CONTAINS");
            DriverInventoryPartRelationship(inventoryParts, parts, "CONTAINS");
        }

        //private static void DriverInventoryPartRelationship(IEnumerable<InventoryPart> inventoryParts, IEnumerable<Part> parts, string relationship1, string relationship2)
        private static void DriverInventoryPartRelationship(IEnumerable<InventoryPart> inventoryParts, IEnumerable<Part> parts, string relationship)
        {
            // TODO: Add Indexes

            //MATCH(:Artist) -[r: RELEASED] - (: Album)
            //DELETE r
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($@"
MATCH(:Inventory) -[r:{relationship}] - (:Part)
DELETE r
                ");
//                deleteResult = driver.ExecuteCypher($@"
//MATCH(:Part) -[r:{relationship2}] - (:Inventory)
//DELETE r
//                ");

                //CREATE INDEX ON: User(username)
                //CREATE INDEX ON: Role(name)
                driver.ExecuteCypher("CREATE INDEX ON: Part(Id)");
                //driver.ExecuteCypher("CREATE INDEX ON: Inventory(Id)");

                var inventoryPartParts = inventoryParts.Join(parts, _ => _.PartId, _ => _.Id, (inventoryPart, part) => new { inventoryPart, part })
                    .Select(_ => new { InventoryId = _.inventoryPart.InventoryId, PartId = _.part.Id, Quantity = _.inventoryPart.Quantity, IsSpare = _.inventoryPart.IsSpare });
                //.GroupBy(_ => new { SetId = _.set.Id, InventoryId = _.inventory.Id });

                foreach (var inventoryPartPart in inventoryPartParts)
                {
                    var insertResult = driver.ExecuteCypher($@"
MATCH (i:Inventory {{Id: '{inventoryPartPart.InventoryId}' }}), (p:Part {{ Id: '{inventoryPartPart.PartId}' }})
CREATE (i)-[:{relationship} {{ Quantity: {inventoryPartPart.Quantity}, IsSpare: {inventoryPartPart.IsSpare} }}]->(p)
                    ");

//                    insertResult = driver.ExecuteCypher($@"
//MATCH (p:Part {{ Id: '{inventoryPartPart.PartId}' }}), (i:Inventory {{Id: '{inventoryPartPart.InventoryId}' }})
//CREATE (p)-[:{relationship2} {{ Quantity: {inventoryPartPart.Quantity}, IsSpare: {inventoryPartPart.IsSpare} }}]->(i)
//                    ");
                }
            }
        }

        //private static void DriverSetInventoryRelationship(IEnumerable<Set> sets, IEnumerable<Inventory> inventories, string relationship1, string relationship2)
        private static void DriverSetInventoryRelationship(IEnumerable<Set> sets, IEnumerable<Inventory> inventories, string relationship)
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($@"
MATCH(:Set) -[r:{relationship}] - (:Inventory)
DELETE r
                ");

                driver.ExecuteCypher("CREATE INDEX ON: Set(Id)");
                driver.ExecuteCypher("CREATE INDEX ON: Inventory(Id)");

                var setInventories = sets.Join(inventories, _ => _.Id, _ => _.SetId, (set, inventory) => new { set, inventory })
                    .Select(_ => new { SetId = _.set.Id, InventoryId = _.inventory.Id, _.inventory.Version });

                foreach (var setInventory in setInventories)
                {
                    var insertResult = driver.ExecuteCypher($@"
MATCH (s:Set {{Id: '{setInventory.SetId}' }}), (i:Inventory {{ Id: '{setInventory.InventoryId}' }})
CREATE (s)-[:{relationship} {{ Version: {setInventory.Version} }}]->(i)
                    ");
                }
            }
        }

        private static void DriverLoadInventory(IEnumerable<Inventory> inventories)
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Inventory) DETACH DELETE a");

                foreach (var inventory in inventories)
                {
                    var insertResult = driver.InsertInventory(inventory);
                }
                // 47 seconds
            }
        }

        private static IEnumerable<Inventory> GetInventories()
        {
            var inventories = ReadCsv.Process(@"C:\temp\BigData\LEGO\inventories.csv")
                .Skip(1)
                .Select(_ => new Inventory()
                {
                    Id = _[0],
                    Version = int.Parse(_[1]),
                    SetId = _[2],
                    
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
                    IsSpare = _[3] == "t" 
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
                var deleteResult = driver.ExecuteCypher("MATCH (a:Part) DETACH DELETE a");

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
                var deleteResult = driver.ExecuteCypher("MATCH (a:Set) DETACH DELETE a");

                foreach (var set in sets)
                {
                    var insertResult = driver.InsertSet(set);
                }
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
