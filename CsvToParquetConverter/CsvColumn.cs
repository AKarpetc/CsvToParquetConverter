using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvToParquetConverter
{
    public class CsvColumn
    {
        public CsvColumn()
        {
            Values = new List<string>();
        }

        public string Header { get; set; }

        public List<string> Values { get; set; }
    }
}
