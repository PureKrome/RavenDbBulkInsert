using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FizzWare.NBuilder.Generators;
using MoreLinq;
using Raven.Client.Documents;

namespace RavenDbBulkInsert
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting bulk insert testing app.");

            var documentStore = new DocumentStore()
            {
                Urls = new[] {"http://localhost:8080"},
                Database = "BulkInsertTest"
            }.Initialize();

            // Create a list of fake employees.
            var employees = Builder<Employee>.CreateListOfSize(1000 * 1000)
            //var employees = Builder<Employee>.CreateListOfSize(100)
                .All()
                .With(x => x.FirstName, GetRandom.FirstName())
                .And(x => x.LastName, GetRandom.LastName())
                .Build();

            Console.WriteLine("Created all fake employees.");

            var batches = employees.Batch(10000);

            var stopwatch = Stopwatch.StartNew();

            try
            {
                foreach (var batch in batches)
                {
                    await BulkInsertEmployees(batch, documentStore);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }

            stopwatch.Stop();

            Console.WriteLine($"Inserted {employees.Count:N0} employee's in {stopwatch.Elapsed.TotalSeconds:N2} seconds.");

            Console.WriteLine("-- Press any key to quit.");
            Console.ReadKey();
        }

        private static async Task BulkInsertEmployees(IEnumerable<Employee> batch, IDocumentStore documentStore)
        {
            using (var operation = documentStore.BulkInsert())
            {
                foreach (var employee in batch)
                {
                    await operation.StoreAsync(employee);
                }

                //var tasks = batch.Select(employee => operation.StoreAsync(employee)).ToArray();

                //await Task.WhenAll(tasks);
            }
        }
    }
}
