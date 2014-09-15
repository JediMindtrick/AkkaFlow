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

    public class HeadBlock : ProcessOperationAsync<object, object>
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

    public class AddBlock : ProcessOperationAsync<int, int>
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

    public class LogBlock : ProcessOperationAsync<object, object>
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

    public class PassBlock : ProcessOperationAsync<object, object>
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

    public class TransformBlock : ProcessOperationAsync<WrapInt, int>
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

    public class ScaleBlock : ProcessOperationAsync<int, object>
    {
        public ScaleBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn)
        {
            f = input =>
            {
                return new Task<object>(() =>
                {
                    //simple action needs to be wrapped in a block
                    var simple = BlockFactory.createSimpleBlock(new List<ActorRef> { BlockFactory.getLogOperation(system, conn) },system,conn);
                    
                    var complex = BlockFactory.getComplexBlock(system, conn);

//                    var block1 = BlockFactory.getAddBlock(system, conn);
  //                  var block2 = BlockFactory.getAddBlock(system, conn);
    //                var block3 = BlockFactory.getAddBlock(system, conn);

      //              var myProcess = BlockFactory.createComplexBlock(new List<ActorRef> { block1, block2, block3 });

                    var myProcess = BlockFactory.createComplexBlock(new List<ActorRef> { simple, complex });

                    myProcess.Tell(input);

                    return input;
                });
            };
        }
    }

}