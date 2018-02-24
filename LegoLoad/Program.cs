﻿using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var greeter = new DriverAdapter("bolt://localhost:7687", "neo4j", "krampus"))
            {
                Console.WriteLine(greeter.CreateGreeting("hello, world"));
            }


        }
    }
}
