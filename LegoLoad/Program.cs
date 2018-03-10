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
        public static int PARALLEL_LIMIT = 5;

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
            var themes = GetThemes();
            var colors = GetColors();
            
            DriverLoadParts(parts);
            DriverLoadSets(sets);
            //DriverLoadInventory(inventories);
            DriverLoadThemes(themes);
            DriverLoadColors(colors);
            DriverSetPartRelationship(sets, inventories, inventoryParts, "CONTAINS");
            DriverSetThemeRelationship(sets, themes, "HAS_THEME");
            DriverThemeThemeRelationship(themes, "IS_PARENT");
            //DriverInventoryPartRelationship(inventoryParts, parts, "CONTAINS");
        }

        private static IEnumerable<Theme> GetThemes()
        {
            var themes = ReadCsv.Process(@"C:\temp\BigData\LEGO\themes.csv")
                .Skip(1)
                .Select(_ => new Theme()
                {
                    Id = int.Parse(_[0]),
                    Name = _[1],
                    ParentId = !string.IsNullOrWhiteSpace(_[2]) ? int.Parse(_[2]) : (int?)null,
                });
            return themes;
        }

        private static IEnumerable<Color> GetColors()
        {
            var colors = ReadCsv.Process(@"C:\temp\BigData\LEGO\colors.csv")
                .Skip(1)
                .Select(_ => new Color()
                {
                    Id = int.Parse(_[0]),
                    Name = _[1],
                    Rgb = _[2],
                    IsTrans = _[3] == "t",
                });
            return colors;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sets"></param>
        /// <param name="inventories"></param>
        /// <param name="relationship"></param>
        private static void DriverSetPartRelationship(IEnumerable<Set> sets, IEnumerable<Inventory> inventories, IEnumerable<InventoryPart> inventoryParts, string relationship)
        {
            var setInventories = sets.Join(inventories, _ => _.Id, _ => _.SetId, (set, inventory) => new { set, inventory })
                .Select(_ => new { SetId = _.set.Id, InventoryId = _.inventory.Id, _.inventory.Version });

            var setInventoryParts = setInventories.Join(inventoryParts, _ => new { InventoryId = _.InventoryId }, _ => new { InventoryId = _.InventoryId.ToString() }, (set, inventoryPart) => new { set, inventoryPart })
                .Select(_ => new { SetId = _.set.SetId, _.set.Version, PartId = _.inventoryPart.PartId, _.inventoryPart.ColorId, _.inventoryPart.IsSpare, _.inventoryPart.Quantity });

            var count = setInventoryParts.Count();

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($@"
MATCH(:Set) -[r:{relationship}] - (:Inventory)
DELETE r
                ");

                driver.ExecuteCypher("CREATE INDEX ON: Set(Id)");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(setInventoryParts, po, (setInventoryPart) =>
                {
                    var insertResult = driver.ExecuteCypher($@"
MATCH (s:Set {{Id: '{setInventoryPart.SetId}' }}), (i:Part {{ Id: '{setInventoryPart.PartId}' }})
CREATE (s)-[:{relationship} {{ Version: {setInventoryPart.Version} }}]->(i)
                    ");
                });

//                foreach (var setInventoryPart in setInventoryParts)
//                {
//                    var insertResult = driver.ExecuteCypher($@"
//MATCH (s:Set {{Id: '{setInventoryPart.SetId}' }}), (i:Part {{ Id: '{setInventoryPart.PartId}' }})
//CREATE (s)-[:{relationship} {{ Version: {setInventoryPart.Version} }}]->(i)
//                    ");
//                }
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
                    ColorId = _[2].As<int>(),
                    Quantity = _[3].As<int>(),
                    IsSpare = _[4] == "t"
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
                    ThemeId = _[3].As<int>(),
                    PartsCount = _[4].As<int>(),
                });
            return sets;
        }

        private static void DriverLoadParts(IEnumerable<Part> parts)
        {
            var nodeName = "Part";
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Part) DETACH DELETE a");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(parts, po, (part) => {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", part.Id.ToString()),
                        new KeyValuePair<string, string>("Description", part.Description),
                    };
                    var insertResult = driver.Insert(nodeName, keyValuePairs);
                });

                //foreach (var part in parts)
                //{
                //    var insertResult = driver.InsertPart(part);
                //}
                // 47 seconds
            }
        }

        private static void DriverThemeThemeRelationship(IEnumerable<Theme> themes, string relationship)
        {
            //var themesThemes = themes.Join(themes, _ => _.ParentId, _ => _.Id, (theme, themeParent) => new { ThemeId = theme.Id, ThemeParentId = themeParent.Id});

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(themes, po, (theme) =>
                {
                    var themeChild = new KeyValuePair<string, string>("Theme", theme.Id.ToString());
                    var themeParent = new KeyValuePair<string, string>("Theme", theme.ParentId.ToString());
                    driver.CreateRelationship_Type_Id(themeChild, themeParent, relationship);
                });

                //foreach (var set in sets)
                //{
                //    var insertResult = driver.InsertSet(set);
                //}
            }
        }

        private static void DriverSetThemeRelationship(IEnumerable<Set> sets, IEnumerable<Theme> themes, string relationship)
        {
            var setsThemes = sets.Join(themes, _ => _.ThemeId, _ => _.Id, (set, theme) => new { set, theme })
                .Select(_ => new { SetId = _.set.Id, ThemeId = _.theme.Id });

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(setsThemes, po, (setTheme) =>
                {
                    var set = new KeyValuePair<string, string>("Set", setTheme.SetId);
                    var theme = new KeyValuePair<string, string>("Theme", setTheme.ThemeId.ToString());
                    driver.CreateRelationship_Type_Id(set, theme, relationship);
                });

                //var deleteResult = driver.ExecuteCypher("MATCH (a:Theme) DETACH DELETE a");

                //ParallelOptions po = new ParallelOptions();
                //po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                //Parallel.ForEach(themes, po, (set) => {
                //    var keyValuePairs = new List<KeyValuePair<string, string>>()
                //    {
                //        new KeyValuePair<string, string>("Id", set.Id.ToString()),
                //        new KeyValuePair<string, string>("Name", set.Name),
                //        new KeyValuePair<string, string>("Year", set.Year.ToString()),
                //    };
                //    var insertResult = driver.Insert(nodeName, keyValuePairs);
                //});

                //foreach (var set in sets)
                //{
                //    var insertResult = driver.InsertSet(set);
                //}
            }
        }

        private static void DriverLoadSets(IEnumerable<Set> sets)
        {
            var nodeName = "Set";

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher("MATCH (a:Set) DETACH DELETE a");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(sets, po, (set) => {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", set.Id.ToString()),
                        new KeyValuePair<string, string>("Name", set.Name),
                        new KeyValuePair<string, string>("Year", set.Year.ToString()),
                    };
                    var insertResult = driver.Insert(nodeName, keyValuePairs);
                });
                
                //foreach (var set in sets)
                //{
                //    var insertResult = driver.InsertSet(set);
                //}
            }
        }

        private static void DriverLoadThemes(IEnumerable<Theme> themes)
        {
            var nodeName = "Theme";
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($"MATCH (a:{nodeName}) DETACH DELETE a");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(themes, po, (theme) =>
                {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", theme.Id.ToString()),
                        new KeyValuePair<string, string>("Name", theme.Name),
                        new KeyValuePair<string, string>("ParentId", theme.ParentId.ToString()),
                    };
                    var insertResult = driver.Insert(nodeName, keyValuePairs);
                });

                //foreach (var theme in themes)
                //{
                //    var keyValuePairs = new List<KeyValuePair<string, string>>()
                //    {
                //        new KeyValuePair<string, string>("Id", theme.Id.ToString()),
                //        new KeyValuePair<string, string>("Name", theme.Name),
                //        new KeyValuePair<string, string>("ParentId", theme.ParentId.ToString()),
                //    };
                //    var insertResult = driver.Insert(nodeName, keyValuePairs);
                //}
            }
        }

        private static void DriverLoadColors(IEnumerable<Color> colors)
        {
            var nodeName = "Color";
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($"MATCH (a:{nodeName}) DETACH DELETE a");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(colors, po, (color) => {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", color.Id.ToString()),
                        new KeyValuePair<string, string>("Name", color.Name),
                        new KeyValuePair<string, string>("Rgb", color.Rgb.ToString()),
                        new KeyValuePair<string, string>("IsTrans", color.IsTrans.ToString()),
                    };
                    var insertResult = driver.Insert(nodeName, keyValuePairs);
                });

                //foreach (var color in colors)
                //{
                //    var keyValuePairs = new List<KeyValuePair<string, string>>()
                //    {
                //        new KeyValuePair<string, string>("Id", color.Id.ToString()),
                //        new KeyValuePair<string, string>("Name", color.Name),
                //        new KeyValuePair<string, string>("Rgb", color.Rgb.ToString()),
                //        new KeyValuePair<string, string>("IsTrans", color.IsTrans.ToString()),
                //    };
                //    var insertResult = driver.Insert(nodeName, keyValuePairs);
                //}
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
