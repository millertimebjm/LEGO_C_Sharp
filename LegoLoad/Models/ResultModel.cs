using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LegoLoad
{
    public class ResultModel
    {
        public string Message { get; set; }
        public object Data { get; set; }
        public bool Result { get; set; }
        public long ExecutionTimeInMilliseconds { get; set; }
        public int RecordsModifiedCount { get; set; }
    }
}
