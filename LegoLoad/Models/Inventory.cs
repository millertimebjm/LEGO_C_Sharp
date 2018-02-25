using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad.Models
{
    public class Inventory
    {
        public string Id { get; set; }
        public string SetId { get; set; }
        public List<Set> Sets { get; set; }
    }
}
