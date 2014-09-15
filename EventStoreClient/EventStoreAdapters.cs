using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using EventStore;
using EventStore.ClientAPI;
using Akka;
using Akka.Actor;

namespace EventStoreClient.EventStoreAdapters
{
    public class EventData {
        public String Name { get; set; }
        public Object MetaData { get; set; }
        public Object Data { get; set; }

        public EventStore.ClientAPI.EventData serialize(){

            var _meta = JsonConvert.SerializeObject(new {
                metadata = MetaData
            });
            var _data = JsonConvert.SerializeObject(Data);
            var _metaBytes = System.Text.Encoding.UTF8.GetBytes(_meta);
            var _dataBytes = System.Text.Encoding.UTF8.GetBytes(_data);

            return new EventStore.ClientAPI.EventData(Guid.NewGuid(), Name, true, _dataBytes, _metaBytes);
        }
    }

    public class SpeakerActor : ReceiveActor {
    
        private IEventStoreConnection Connection = null;
        private String StreamName = String.Empty;

        public SpeakerActor(IEventStoreConnection connection, String streamName) {
            
            Connection = connection;
            StreamName = streamName;
         
            Receive<EventData>(msg =>
                connection.AppendToStream(StreamName,EventStore.ClientAPI.ExpectedVersion.Any,msg.serialize()));
        }
    }

    public class EventListener : ReceiveActor
    {
        private IEventStoreConnection Connection = null;
        private String StreamName = String.Empty;
        private Dictionary<String, Type> TypeMap = null;

        public EventListener(IEventStoreConnection connection, String streamName, Dictionary<String,Type> typeMap, ActorRef outputSink)
        {
            Connection = connection;
            StreamName = streamName;
            TypeMap = typeMap;

            Connection.SubscribeToStream(StreamName,false,(sub, evt) => {

                if (TypeMap.ContainsKey(evt.Event.EventType))
                {
                    var msg = JsonConvert.DeserializeObject(System.Text.Encoding.UTF8.GetString(evt.Event.Data), TypeMap[evt.Event.EventType]);

                    outputSink.Tell(msg);
                }
                else {
                    throw new Exception("Unable to deserialize event type: " + evt.Event.EventType);
                }

            });
        }
    }
}
