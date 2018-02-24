using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.FileIO;

namespace LegoLoad
{
    public static class ReadCsv
    {
        public static IEnumerable<string[]> Process(string path)
        {
            using (TextFieldParser csvParser = new TextFieldParser(path))
            {
                var data = new List<string[]>();

                csvParser.CommentTokens = new string[] { "#" };
                csvParser.SetDelimiters(new string[] { "," });
                csvParser.HasFieldsEnclosedInQuotes = true;

                // Skip the row with the column names
                //csvParser.ReadLine();

                while (!csvParser.EndOfData)
                {
                    data.Add(csvParser.ReadFields());
                }

                return data;
            }
        }
    }
}
