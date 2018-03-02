using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad.Models
{
    public class InventoryPart
    {
        public int InventoryId { get; set; }
        public string PartId { get; set; }
        public int Quantity { get; set; }
        public bool IsSpare { get; set; }
    }
}
