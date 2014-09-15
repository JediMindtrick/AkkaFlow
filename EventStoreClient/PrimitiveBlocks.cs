using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Newtonsoft.Json;
using EventStoreClient.EventStoreAdapters;

namespace EventStoreClient
{

    public class HeadBlock : ProcessBlockAsync<object, object>
    {
        public HeadBlock()
        {
            f = input =>
            {
                return new Task<object>(() =>
                {
                    Console.WriteLine("Head: " + JsonConvert.SerializeObject(input));
                    return input;
                });
            };
        }
    }

    public class AddBlock : ProcessBlockAsync<int, int>
    {
        public int howMany { get; set; }
        private int howLong = 2000;

        public AddBlock(int _toAdd)
        {
            howMany = _toAdd;

            f = lh =>
            {
                return new Task<int>(() =>
                {
                    Console.WriteLine("Add: {0} + {1}", lh, howMany);
                    Console.WriteLine("Long Running: {0}", howLong);
                    System.Threading.Thread.Sleep(howLong);
                    Console.WriteLine("Add Result: {0}", (lh + howMany));
                    return lh + howMany;
                });
            };
        }
    }

    public class LogBlock : ProcessBlockAsync<object, object>
    {
        public LogBlock()
        {
            f = input =>
            {
                journal(new EventData { Name = "BeginLog", Data = input });
                return new Task<object>(() =>
                {
                    Console.WriteLine("Log: " + JsonConvert.SerializeObject(input));
                    journal(new EventData { Name = "EndLog", Data = input });

                    return input;
                });
            };
        }
    }

    public class PassBlock : ProcessBlockAsync<object, object>
    {
        public PassBlock()
        {
            f = input =>
            {
                return new Task<object>(() =>
                {
                    Console.WriteLine("Pass: " + JsonConvert.SerializeObject(input));
                    return input;
                });
            };
        }
    }

    public class TransformBlock : ProcessBlockAsync<WrapInt, int>
    {
        public TransformBlock()
        {
            f = input =>
            {
                return new Task<int>(() =>
                {
                    Console.WriteLine("Transform: " + JsonConvert.SerializeObject(input));

                    return input.Data;
                });
            };
        }
    }

    public class ScaleBlock : ProcessBlockAsync<int, object>
    {
        public ScaleBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn)
        {
            f = input =>
            {
                return new Task<object>(() =>
                {

                    var myProcess = BlockFactory.getComplexBlock(system, conn);

                    myProcess.Tell(input);

                    return input;
                });
            };
        }
    }

}