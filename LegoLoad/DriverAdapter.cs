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

        public ResultModel InsertPart(Part part)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                //CREATE (user:User { Id: 456, Name: 'Jim' })
                //session.WriteTransaction()


                session.WriteTransaction(tx =>
                {
                    var result = tx.Run("CREATE (part:Part {Id: $Id, Description: $Description})",
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

        internal ResultModel InsertSet(Set set)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                //CREATE (user:User { Id: 456, Name: 'Jim' })
                //session.WriteTransaction()

                session.WriteTransaction(tx =>
                {
                    var result = tx.Run("CREATE (set:Set {Id: $Id, Name: $Name, Year: $Year })",
                        new { set.Id, set.Name, set.Year });
                });

                return new ResultModel()
                {
                    Data = null,
                    Result = true,
                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                };
            }
        }

        internal ResultModel ExecuteCypher(string cypher)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                session.WriteTransaction(tx =>
                {
                    var result = tx.Run(cypher);
                });

                return new ResultModel()
                {
                    Data = null,
                    Result = true,
                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                };
            }
        }

        public ResultModel GetPart(string id)
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                var node = session.ReadTransaction<INode>(tx =>
                {
                    var result = tx.Run(@"
MATCH (part:Part)
WHERE part.Id = $Id
RETURN part
                    ", new { Id = id });
                    return result.Single()[0].As<INode>();
                });

                return new ResultModel()
                {
                    Data = node.AsPart(),
                    ExecutionTimeInMilliseconds = StopwatchEndAndElapsed(),
                };
            }
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
    }
}
