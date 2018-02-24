using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad
{
    class DriverAdapter : IDisposable
    {
        private readonly IDriver _driver;

        public DriverAdapter(string uri, string user, string password)
        {
            _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
        }

        public string CreateGreeting(string message)
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
                return greeting;
            }
        }

        public void Dispose()
        {
            _driver?.Dispose();
        }
    }
}
