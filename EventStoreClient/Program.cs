using Akka.Actor;
using EventStoreClient.EventStoreAdapters;
using System;
using System.Collections.Generic;

namespace EventStoreClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var conn = EventStore.ClientAPI.EventStoreConnection.Create(new System.Net.IPEndPoint(System.Net.IPAddress.Parse("192.168.164.132"), 1113));
            conn.Connect();
            
            var system = ActorSystem.Create("MySystem");
            
            var sink = system.ActorOf(Props.Create(() => new TransformBlock() ), Guid.NewGuid().ToString());

            var scale = system.ActorOf(Props.Create(() => new ScaleBlock(system, conn)), "scale");

            sink.Tell(new Messages.SetNext { Next = scale });

            var listener = system.ActorOf(Props.Create(() => new EventListener(
                conn,
                "test-event-2",
                new Dictionary<string, Type> { 
                                { "WrapInt", typeof(WrapInt)}
                            },
                sink)));

            var speaker = system.ActorOf(Props.Create(() => new SpeakerActor(conn, "test-event-2")), "speaker");

            speaker.Tell(new EventData { Name = "WrapInt", Data = new WrapInt { Data = 1 }, MetaData = null });

            /*
            for (int i = 0, l = 10; i < l; i++) {
                speaker.Tell(new EventData { Name = "WrapInt", Data = new WrapInt { Data = i }, MetaData = null });
            }
            */

            Console.ReadLine();
        }
    }
}
