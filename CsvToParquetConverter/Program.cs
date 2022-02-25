using Microsoft.Extensions.Configuration;
using Parquet;
using Parquet.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;

namespace CsvToParquetConverter
{
    internal class Program
    {
        static IConfiguration config;

        public static void SetUpConfig()
        {
            config = new ConfigurationBuilder()
                       .AddJsonFile("appsettings.json")
                       .AddJsonFile($"appsettings.Development.json", true, true)
                       .AddEnvironmentVariables()
                       .Build();
        }


        static void Main(string[] args)
        {
            SetUpConfig();

            var filesFolder = config["FilesFolder"];

            Console.WriteLine("Enter the name of file:");

            var nameOfFile = Console.ReadLine();

            if (nameOfFile.Contains(".csv", StringComparison.OrdinalIgnoreCase))
                nameOfFile = nameOfFile.Replace(".csv", "");

            List<CsvColumn> columns = new List<CsvColumn>();

            using (var reader = new StreamReader($@"{filesFolder}{nameOfFile}.csv"))
            {
                bool isHeader = true;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';');

                    if (isHeader)
                    {
                        columns.AddRange(values.Select(x => new CsvColumn { Header = x }));
                        isHeader = false;
                        continue;
                    }

                    for (int i = 0; i < values.Length; i++)
                    {
                        columns[i].Values.Add(values[i]);
                    }
                }
            }

            var parquetColums = new List<DataColumn>();

            foreach (var column in columns)
            {
                TryDefinType(column.Values.FirstOrDefault(x => !string.IsNullOrEmpty(x)), out var type, out var value);
                var typeConverter = new TypeConverter();

                var arrayValues = new ArrayList();
                foreach (var newValue in column.Values)
                {
                    TryDefinType(newValue, out var valueType, out var result);

                    if (result == null)
                    {
                        arrayValues.Add(result);
                        continue;
                    }

                    var castedResult = Convert.ChangeType(result, valueType);

                    arrayValues.Add(castedResult);
                }

                var dataColumn = new DataColumn(new DataField(column.Header, GetNullableType(type)), arrayValues.ToArray(GetNullableType(type)));
                parquetColums.Add(dataColumn);
            }

            var schema = new Schema(parquetColums.Select(x => x.Field).ToList());

            using (Stream fileStream = System.IO.File.Create($@"{filesFolder}{nameOfFile}.parquet"))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var column in parquetColums)
                        {
                            groupWriter.WriteColumn(column);
                        }
                    }
                }
            }
        }

        static void TryDefinType(string input, out Type type, out object value)
        {
            if (DateTimeOffset.TryParse(input, out var datetime))
            {
                value = datetime;
                type = datetime.GetType();
                return;
            }

            if (int.TryParse(input, out var intNumber))
            {
                value = intNumber;
                type = intNumber.GetType();
                return;
            }

            if (decimal.TryParse(input, out var floatNumber))
            {
                value = floatNumber;
                type = floatNumber.GetType();
                return;
            }

            if (string.IsNullOrEmpty(input?.Trim()))
            {
                value = null;
                type = typeof(string);
                return;
            }

            type = typeof(string);
            value = input;
        }

        static Type GetNullableType(Type type)
        {
            // Use Nullable.GetUnderlyingType() to remove the Nullable<T> wrapper if type is already nullable.
            type = Nullable.GetUnderlyingType(type) ?? type; // avoid type becoming null
            if (type.IsValueType)
                return typeof(Nullable<>).MakeGenericType(type);
            else
                return type;
        }

    }
}
