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
            catch(Exception ex)
            {
                return new ResultModel()
                {
                    Result = false,
                    Message = ex.Message,
                };
            }
            
        }

        public ResultModel InsertNode()
        {
            StopwatchResetAndStart();
            using (var session = _driver.Session())
            {
                //CREATE (user:User { Id: 456, Name: 'Jim' })
                //session.WriteTransaction()
                var part = new Part()
                {
                    Id = "10039",
                    Description = "Pullback Motor 8 x 4 x 2/3",
                };

                session.WriteTransaction(tx =>
                {
                    var result = tx.Run("CREATE (part:Part {Id: $Id, Description: $Description})",
                        new { part.Id, part.Description });
                });

//Neo4j.Driver.V1.ClientException: 'Invalid input '{ ': expected whitespace, a property key name, '}
//', an identifier or UnsignedDecimalInteger (line 1, column 20 (offset: 19))
//"CREATE (part:Part {{Id: $Id, Description: $Description}})"
//         


                var part2 = session.ReadTransaction(tx =>
                {
                    var result = tx.Run(@"
MATCH (part:Part)
WHERE part.Id = $Id
RETURN part
                    ", new { part.Id });
                    return result.Single()[0].As<Part>();
                });

                return new ResultModel()
                {
                    Data = part2,
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
