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
    public class Messages
    {

        public class Result<R>
        {
            public R Data { get; set; }
            public bool EndBlock { get; set; }
        }

        public class SetNext
        {
            public ActorRef Next { get; set; }
        }

        public class SetHead
        {
            public ActorRef Head { get; set; }
        }

        public class SetNextBlock
        {
            public ActorRef NextBlock { get; set; }
        }

        public class MarkTail { }
        public class MarkHead
        {
            public ActorRef Stream { get; set; }
        }
        public class BeginBlock<T>
        {
            public T Data { get; set; }
        }
        public class EndBlock<T>
        {
            public T Data { get; set; }
        }

        public class Journal
        {
            public EventData Record { get; set; }
        }
    }
}