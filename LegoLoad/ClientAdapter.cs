using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Neo4jClient;

namespace LegoLoad
{
    public class ClientAdapter
    {
        private GraphClient _client;
        
        public Neo4jClient.Cypher.ICypherFluentQuery Cypher { get; set; }

        public ClientAdapter(string url, string username, string password)
        {
            _client = new GraphClient(new Uri(url), username, password);
            _client.Connect();
        }
    }
}
