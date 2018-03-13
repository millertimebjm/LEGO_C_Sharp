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
        ///     CREATE ([alias]:[Table] {[column1]: '[value1]', [column2]: '[value2]', ... })
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            //DriverAdapter();
            //ClientAdapter();
            Console.WriteLine("Get Parts Started...");
            var parts = GetParts();
            Console.WriteLine("Get Parts Completed.");
            Console.WriteLine("Get Sets Started...");
            var sets = GetSets();
            Console.WriteLine("Get Sets Completed.");
            Console.WriteLine("Get Inventories Started...");
            var inventories = GetInventories();
            Console.WriteLine("Get Inventories Completed.");
            Console.WriteLine("Get InventoryParts Started...");
            var inventoryParts = GetInventoryParts();
            Console.WriteLine("Get InventoryParts Completed.");
            Console.WriteLine("Get Themes Started...");
            var themes = GetThemes();
            Console.WriteLine("Get Themes Completed.");
            Console.WriteLine("Get Colors Started...");
            var colors = GetColors();
            Console.WriteLine("Get Colors Completed.");

            Console.WriteLine("Get Neo4j Delete All Started...");
            DriverDeleteAll();
            Console.WriteLine("Get Neo4j Delete All Completed");

            //DriverLoadColors(colors);
            Console.WriteLine("Get Neo4j Load Part Nodes Started...");
            DriverLoadParts(parts);
            Console.WriteLine("Get Neo4j Load Part Nodes Completed.");
            Console.WriteLine("Get Neo4j Load Set Nodes Started...");
            DriverLoadSets(sets);
            Console.WriteLine("Get Neo4j Load Set Nodes Completed.");
            Console.WriteLine("Get Neo4j Load Theme Nodes Started...");
            DriverLoadThemes(themes);
            Console.WriteLine("Get Neo4j Load Theme Nodes Completed...");

            Console.WriteLine("Get Neo4j Load Set Part Relationship Started...");
            DriverSetPartRelationship(sets, inventories, inventoryParts, colors, "CONTAINS");
            Console.WriteLine("Get Neo4j Load Set Part Relationship Completed.");
            Console.WriteLine("Get Neo4j Load Set Theme Relationship Started...");
            DriverSetThemeRelationship(sets, themes, "HAS_THEME");
            Console.WriteLine("Get Neo4j Load Set Theme Relationship Completed.");
            Console.WriteLine("Get Neo4j Load Theme Theme Relationship Started...");
            DriverThemeThemeRelationship(themes, "IS_PARENT");
            Console.WriteLine("Get Neo4j Load Theme Theme Relationship Completed.");
        }

        private static IEnumerable<Theme> GetThemes()
        {
            var themes = ReadCsv.Process(@"files\themes.csv")
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
            var colors = ReadCsv.Process(@"files\colors.csv")
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

        private static void DriverDeleteAll()
        {
            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                driver.ExecuteCypher("MATCH (r) DETACH DELETE r");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sets"></param>
        /// <param name="inventories"></param>
        /// <param name="relationship"></param>
        private static void DriverSetPartRelationship(IEnumerable<Set> sets, IEnumerable<Inventory> inventories, IEnumerable<InventoryPart> inventoryParts, IEnumerable<Color> colors, string relationship)
        {
            var setInventories = sets.Join(inventories, _ => _.Id, _ => _.SetId, (set, inventory) => new { set, inventory })
                .Select(_ => new { SetId = _.set.Id, InventoryId = _.inventory.Id, _.inventory.Version });

            var setInventoryParts = setInventories.Join(inventoryParts, _ => new { InventoryId = _.InventoryId }, _ => new { InventoryId = _.InventoryId.ToString() }, (set, inventoryPart) => new { set, inventoryPart })
                .Select(_ => new { _.set.SetId, _.set.Version, _.inventoryPart.PartId, _.inventoryPart.ColorId, _.inventoryPart.IsSpare, _.inventoryPart.Quantity });

            var setInventoryPartColors = setInventoryParts.Join(colors, _ => _.ColorId, _ => _.Id, (setInventoryPart, color) => new { setInventoryPart, color })
                .Select(_ => new { _.setInventoryPart.SetId, _.setInventoryPart.Version, _.setInventoryPart.PartId, _.setInventoryPart.IsSpare, _.setInventoryPart.Quantity, ColorName = _.color.Name, _.color.Rgb, _.color.IsTrans });

            var count = setInventoryParts.Count();

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($@"
MATCH(:Set) -[r:{relationship}] - (:Inventory)
DELETE r
                ");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(setInventoryPartColors, po, (setInventoryPartColor) =>
                {
                    //                    var insertResult = driver.ExecuteCypher($@"
                    //MATCH (s:Set {{Id: '{setInventoryPart.SetId}' }}), (i:Part {{ Id: '{setInventoryPart.PartId}' }})
                    //CREATE (s)-[:{relationship} {{ Version: {setInventoryPart.Version},  }}]->(i)
                    //                    ");

                    var setPair = new KeyValuePair<string, string>("Set", setInventoryPartColor.SetId.ToString());
                    var partPair = new KeyValuePair<string, string>("Part", setInventoryPartColor.PartId.ToString());
                    var attributes = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Version", setInventoryPartColor.Version.ToString()),
                        new KeyValuePair<string, string>("IsSpare", setInventoryPartColor.IsSpare.ToString()),
                        new KeyValuePair<string, string>("Quantity", setInventoryPartColor.Quantity.ToString()),
                        new KeyValuePair<string, string>("ColorName", setInventoryPartColor.ColorName),
                        new KeyValuePair<string, string>("Rgb", setInventoryPartColor.Rgb),
                        new KeyValuePair<string, string>("IsTrans", setInventoryPartColor.IsTrans.ToString()),
                    };
                    var insertResult = driver.CreateRelationship_Type_Id(setPair, partPair, relationship, attributes);
                });
            }
        }

        private static IEnumerable<Inventory> GetInventories()
        {
            var inventories = ReadCsv.Process(@"files\inventories.csv")
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
            var inventoryParts = ReadCsv.Process(@"files\inventory_parts.csv")
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
            var parts = ReadCsv.Process(@"files\parts.csv")
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
            var sets = ReadCsv.Process(@"files\sets.csv")
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

                Parallel.ForEach(parts, po, (part) =>
                {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", part.Id.ToString()),
                        new KeyValuePair<string, string>("Description", part.Description),
                    };
                    var insertResult = driver.InsertNode(nodeName, keyValuePairs);
                });

                driver.ExecuteCypher($"CREATE INDEX ON: {nodeName}(Id)");
            }
        }

        private static void DriverThemeThemeRelationship(IEnumerable<Theme> themes, string relationship)
        {
            //var themesThemes = themes.Join(themes, _ => _.ParentId, _ => _.Id, (theme, themeParent) => new { ThemeId = theme.Id, ThemeParentId = themeParent.Id});

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($@"
MATCH(:Theme) -[r:{relationship}] - (:Theme)
DELETE r
                ");

                var beforeCount = driver.GetRelationshipCount(relationship);

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(themes, po, (theme) =>
                {
                    var themeChild = new KeyValuePair<string, string>("Theme", theme.Id.ToString());
                    var themeParent = new KeyValuePair<string, string>("Theme", theme.ParentId.ToString());
                    driver.CreateRelationship_Type_Id(themeChild, themeParent, relationship, null);
                });

                var afterCount = driver.GetRelationshipCount(relationship);
                if (afterCount != themes.Count(_ => _.ParentId != null))
                    throw new Exception("Theme -> Theme Parent relationship count does not match.");
            }
        }

        private static void DriverSetThemeRelationship(IEnumerable<Set> sets, IEnumerable<Theme> themes, string relationship)
        {
            var setsThemes = sets.Join(themes, _ => _.ThemeId, _ => _.Id, (set, theme) => new { set, theme })
                .Select(_ => new { SetId = _.set.Id, ThemeId = _.theme.Id });

            using (var driver = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                var deleteResult = driver.ExecuteCypher($@"
MATCH(:Set) -[r:{relationship}] - (:Theme)
DELETE r
                ");

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = PARALLEL_LIMIT;

                Parallel.ForEach(setsThemes, po, (setTheme) =>
                {
                    var set = new KeyValuePair<string, string>("Set", setTheme.SetId);
                    var theme = new KeyValuePair<string, string>("Theme", setTheme.ThemeId.ToString());
                    driver.CreateRelationship_Type_Id(set, theme, relationship, null);
                });
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

                Parallel.ForEach(sets, po, (set) =>
                {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", set.Id.ToString()),
                        new KeyValuePair<string, string>("Name", set.Name),
                        new KeyValuePair<string, string>("Year", set.Year.ToString()),
                    };
                    var insertResult = driver.InsertNode(nodeName, keyValuePairs);
                });

                driver.ExecuteCypher($"CREATE INDEX ON: {nodeName}(Id)");
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
                    var insertResult = driver.InsertNode(nodeName, keyValuePairs);
                });

                driver.ExecuteCypher($"CREATE INDEX ON: {nodeName}(Id)");
                driver.ExecuteCypher($"CREATE INDEX ON: {nodeName}(ParentId)");
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

                Parallel.ForEach(colors, po, (color) =>
                {
                    var keyValuePairs = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>("Id", color.Id.ToString()),
                        new KeyValuePair<string, string>("Name", color.Name),
                        new KeyValuePair<string, string>("Rgb", color.Rgb.ToString()),
                        new KeyValuePair<string, string>("IsTrans", color.IsTrans.ToString()),
                    };
                    var insertResult = driver.InsertNode(nodeName, keyValuePairs);
                });

                driver.ExecuteCypher($"CREATE INDEX ON: {nodeName}(Id)");
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
