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
    public class ProcessOperationAsync<T,R> : ReceiveActor
    {

        //head block needs to be special, becuase it needs to have another actor ref to next block
        //inside head block's Receive<R>, it needs to forward to the next block
        //so .next points to next actor inside the block
        //.nextBlock points to the head of the next block!!
        public ActorRef next { get; set; }
        public ActorRef nextBlock { get; set; }
        public ActorRef stream { get; set; }
        public ActorRef myHead { get; set; }
        public Func<T, Task<R>> f { get; set; }
        public Boolean isTail = false;
        public Boolean isHead = false;

        private bool verbose = false;

        public ProcessOperationAsync(){

            Receive<Messages.SetNext>(msg => {
                if(verbose) Console.WriteLine(this.Self.Path + " -> next -> " + msg.Next.Path);

                this.next = msg.Next; 
            });

            Receive<Messages.SetNextBlock>(msg =>
            {
                if (verbose) Console.WriteLine(this.Self.Path + " -> nextBlock -> " + msg.NextBlock.Path);
                this.nextBlock = msg.NextBlock;
            });

            Receive<Messages.SetHead>(msg => {
                this.myHead = msg.Head;
            });

            Receive<Messages.MarkTail>(_ =>
            {
                if (verbose) Console.WriteLine(this.Self.Path + " is tail");
                this.isTail = true;
            });

            Receive<Messages.MarkHead>(msg =>
            {
                if (verbose) Console.WriteLine(this.Self.Path + " is head");
                //open an event stream
                this.stream = msg.Stream;
                this.isHead = true;
            });

            Receive<Messages.Journal>(msg => {
                this.journal(msg.Record);
            });

            Receive<Messages.Result<R>>(result =>
            {
                journal(new EventData { Name = "EndOp", Data = result.Data, MetaData = null });

                if (isHead && result.EndBlock) {
                    if (verbose) Console.WriteLine(this.Self.Path + " block finished");

                    var _data = new EventData { Name = "EndBlock", Data = result.Data, MetaData = null };
                    journal(_data);
                }

                if (!result.EndBlock && next != null)
                {
                    next.Tell(result.Data);
                }
                else if (isHead && result.EndBlock && nextBlock != null) {                    
                    nextBlock.Tell(result.Data);
                }
            });

            Receive<T>(input => {
                if (verbose) Console.WriteLine(this.Self.Path + " processing result");
                if (isHead) {
                    var _data = new EventData { Name = "BeginBlock", Data = input, MetaData = null };
                    journal(_data);
                }

                journal(new EventData { Name = "BeginOp", Data = input, MetaData = null });

                run(input);
            });
        }

        protected void journal(EventData toRecord) {
            if (isHead)
            {
                stream.Tell(toRecord);
            }
            
            if(myHead != null) {
                myHead.Tell(new Messages.Journal { Record = toRecord });
            }
        }

        private void run(T input)
        {
            var promise = f(input);
            promise.Start();
            promise.Wait();

            if (!isTail)
            {
                this.Self.Tell(new Messages.Result<R> { Data = promise.Result, EndBlock = false });
            }
            else {
                next.Tell(new Messages.Result<R> { Data = promise.Result, EndBlock = true });
            }
        }
    }
}