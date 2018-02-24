using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad
{
    public static partial class Object
    {
        public static Part AsPart(this INode node)
        {
            return new Part()
            {
                Id = node["Id"].As<string>(),
                Description = node["Description"].As<string>(),
            };
        }

        public static Set AsSet(this INode node)
        {
            return new Set()
            {
                Id = node["Id"].As<string>(),
                Name = node["Name"].As<string>(),
                Year = node["Year"].As<int>(),
            };
        }
    }

    //public static partial class ValueExtensions
    //{
    //    private Part ConvertArrayToPart(string[] o)
    //    {
    //        return new Part()
    //        {
    //            Id = o["Id"].As<string>(),
    //            Description = o["Description"].As<string>(),
    //        };
    //    }

    //    public static T As<T>(this object value)
    //    {
    //        if (typeof(T) == typeof(Part))
    //        {
    //            return (Part)value;
    //        }

            
    //    }
    //}
}
