﻿using LegoLoad.Models;
using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad
{
    public class DriverAdapter : IDisposable
    {
        private readonly IDriver _driver;
        private Stopwatch _stopwatch;

        public DriverAdapter(string uri, string user, string password)
        {
            _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
            _stopwatch = new Stopwatch();
        }

        public ResultModel CreateGreeting(string message)
        {
            StopwatchResetAndStart();
            try
            {
                using (var session = _driver.Session())
                {
                    var greeting = session.WriteTransaction(tx =>
                    {
                        var result = tx.Run("CREATE (a:Greeting) " +
                                            "SET a.message = $message " +
                                            "RETURN a.message + ', from node ' + id(a)",
                            new { message });
                        return result.Single()[0].As<string>();
                    });
                    return new ResultModel()
                    {
                        Message = greeting,
                        ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                        Result = true,
                    };
                }
            }
            catch (Exception ex)
            {
                return new ResultModel()
                {
                    Result = false,
                    Message = ex.Message,
                };
            }

        }

        public ResultModel InsertInventory(Inventory inventory)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                //CREATE (user:User { Id: 456, Name: 'Jim' })
                //session.WriteTransaction()


                session.WriteTransaction(tx =>
                {
                    var result = tx.Run("CREATE (a:Inventory {Id: $Id, Version: $Version })",
                        new { inventory.Id, inventory.Version});
                });

                return new ResultModel()
                {
                    Data = null,
                    Result = true,
                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                };
            }
        }

        public ResultModel InsertPart(Part part)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                //CREATE (user:User { Id: 456, Name: 'Jim' })
                //session.WriteTransaction()


                session.WriteTransaction(tx =>
                {
                    var result = tx.Run(@"CREATE (a:Part {Id: $Id, Description: $Description})",
                        new { part.Id, part.Description });
                });

                return new ResultModel()
                {
                    Data = null,
                    Result = true,
                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                };
            }
        }

        internal ResultModel ExecuteCypher(string cypher, object parameters = null)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                var result = session.WriteTransaction<IStatementResult>(tx =>
                {
                    return tx.Run(cypher, parameters);
                });

                return new ResultModel()
                {
                    Data = result,
                    Result = true,
                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                };
            }
        }

//        public ResultModel GetPart(string id)
//        {
//            StopwatchResetAndStart();
//            using (var session = _driver.Session())
//            {
//                var node = session.ReadTransaction(tx =>
//                {
//                    var result = tx.Run(@"
//MATCH (a:Part)
//WHERE a.Id = $Id
//RETURN a
//                    ", new { Id = id });
//                    return result.Single()[0].As<INode>();
//                });

//                return new ResultModel()
//                {
//                    Data = node,
//                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
//                };
//            }
//        }

        /// <summary>
        /// First Type, then Id
        /// ex. [Set, 1], [Part, 2]
        /// </summary>
        /// <param name="a">Type/Id</param>
        /// <param name="b">Type/Id</param>
        /// <returns></returns>
        public ResultModel CreateRelationship_Type_Id(KeyValuePair<string, string> a, KeyValuePair<string,string> b, string relationType, IEnumerable<KeyValuePair<string, string>> attributes)
        {
            //MATCH(a: Person),(b: Person)
            //WHERE a.name = 'Node A' AND b.name = 'Node B'
            //CREATE(a) -[r: RELTYPE]->(b)
            //RETURN r
            var attributeString = "";
            if (attributes != null)
                attributeString = $"{string.Join(",", attributes.Select(_ => _.Key + ": '" + _.Value.Replace("\\", "\\\\").Replace("'", "\\'") + "'"))}";
            return ExecuteCypher($@"
MATCH(a: { a.Key}),(b: { b.Key})
WHERE a.Id = '{a.Value}' AND b.Id = '{b.Value}'
CREATE(a) -[r: {relationType} {{ {attributeString} }}]->(b)
return r
                    ");
        }

        public void Dispose()
        {
            _driver?.Dispose();
        }

        private void StopwatchResetAndStart()
        {
            _stopwatch.Restart();
        }

        private long StopwatchEndAndElapsed()
        {
            _stopwatch.Stop();
            return _stopwatch.ElapsedMilliseconds;
        }

        internal bool InsertNode(string nodeName, IEnumerable<KeyValuePair<string, string>> attributes)
        {
            var attributeString = $"{string.Join(",", attributes.Select(_ => _.Key + ": '" + _.Value.Replace("\\", "\\\\").Replace("'", "\\'") + "'"))}";
            var result = ExecuteCypher($"CREATE (n:{nodeName} {{ {attributeString} }})");
            return result.Result;
        }

        internal int GetRelationshipCount(string relationship)
        {
            var result = ExecuteCypher($"MATCH ()-[r:{relationship}]->() RETURN COUNT(r)");
            if (result.Result)
                return result.Data.Single()[0].As<int>();
            else
                throw new Exception(result.Message);
        }
    }
}
