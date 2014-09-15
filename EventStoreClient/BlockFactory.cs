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
    public class Wrapper<T>
    {
        public T Data { get; set; }
    }

    public class WrapInt : Wrapper<int> { }

    public class BlockFactory : ProcessBlockAsync<int, int>
    {

        private static int counter = 0;

        public static ActorRef getAddBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn)
        {

            var name = "block" + Guid.NewGuid().ToString();
            var head = system.ActorOf(Props.Create(() => new HeadBlock()), name);

            var speaker = system.ActorOf(Props.Create(() => new SpeakerActor(conn, name)), "speak" + counter++);
            head.Tell(new Messages.MarkHead { Stream = speaker });

            var log1 = system.ActorOf(Props.Create(() => new LogBlock()), "log" + counter++);
            log1.Tell(new Messages.SetHead { Head = head });

            var add1 = system.ActorOf(Props.Create(() => new AddBlock(1)), "add" + counter++);
            add1.Tell(new Messages.SetHead { Head = head });

            var log2 = system.ActorOf(Props.Create(() => new LogBlock()), "log" + counter++);
            log2.Tell(new Messages.SetHead { Head = head });

            head = joinOps(head, log1);
            joinOps(log1, add1);
            joinOps(add1, log2);
            head = endBlock(head, log2);

            return head;
        }

        public static ActorRef getComplexBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn)
        {

            var block1 = BlockFactory.getAddBlock(system, conn);
            var block2 = BlockFactory.getAddBlock(system, conn);
            var block3 = BlockFactory.getAddBlock(system, conn);

            return createComplexBlock(new List<ActorRef> { block1, block2, block3 });
        }

        public static ActorRef createComplexBlock(List<ActorRef> ops)
        {
            var head = ops.First();

            for (int i = 0, l = ops.Count; i < l; i++)
            {
                var _next = i + 1 < ops.Count ? ops[i + 1] : null;
                if (_next != null)
                {
                    _next.Tell(new Messages.SetHead { Head = head });
                    joinBlocks(ops[i], _next);
                }
            }

            return head;
        }

        public static ActorRef joinOps(ActorRef first, ActorRef second)
        {
            first.Tell(new Messages.SetNext { Next = second });
            return first;
        }

        public static ActorRef endBlock(ActorRef head, ActorRef tail)
        {
            tail.Tell(new Messages.SetNext { Next = head });
            tail.Tell(new Messages.MarkTail());

            return head;
        }

        public static ActorRef joinBlocks(ActorRef first, ActorRef second)
        {

            first.Tell(new Messages.SetNextBlock { NextBlock = second });

            return second;
        }
    }
}
