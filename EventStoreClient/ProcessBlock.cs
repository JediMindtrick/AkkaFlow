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

    public class Messages {

        public class Result<R>
        {
            public R Data { get; set; }
            public bool EndBlock { get; set; }
        }

        public class SetNext { 
            public ActorRef Next { get; set; }
        }

        public class SetHead {
            public ActorRef Head { get; set; }
        }

        public class SetNextBlock {
            public ActorRef NextBlock { get; set; }
        }

        public class MarkTail { }
        public class MarkHead {
            public ActorRef Stream { get; set; }
        }
        public class BeginBlock<T> {
            public T Data { get; set; }
        }
        public class EndBlock<T> {
            public T Data { get; set; }
        }

        public class Journal {
            public EventData Record { get; set; }
        }
    }

    public class ProcessBlockAsync<T,R> : ReceiveActor
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

        public ProcessBlockAsync(){

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

    public class HeadBlock : ProcessBlockAsync<object,object>
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

    public class AddBlock : ProcessBlockAsync<int, int> {
        public int howMany { get; set; }
        private int howLong = 2000;
        
        public AddBlock(int _toAdd) {
            howMany = _toAdd;

            f = lh => { 
                return new Task<int>(() => {
                    Console.WriteLine("Add: {0} + {1}", lh, howMany);
                    Console.WriteLine("Long Running: {0}", howLong);
                    System.Threading.Thread.Sleep(howLong);
                    Console.WriteLine("Add Result: {0}", (lh + howMany));
                    return lh + howMany;
                });
            };
        }
    }

    public class LogBlock : ProcessBlockAsync<object, object> {
        public LogBlock() {
            f = input =>
            {
                journal(new EventData { Name = "BeginLog", Data = input });
                return new Task<object>(() => {
                    Console.WriteLine("Log: " + JsonConvert.SerializeObject(input));
                    journal(new EventData { Name = "EndLog", Data = input });

                    return input;
                });
            };
        }
    }

    /*used to be head block implementation*/
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

    public class ScaleBlock : ProcessBlockAsync<int, object> {
        public ScaleBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn)
        {
            f = input => {
                return new Task<object>(() => {

                    var myProcess = BlockFactory.getComplexBlock(system, conn);

                    myProcess.Tell(input);

                    return input;
                });
            };
        }
    }

    public class BlockFactory : ProcessBlockAsync<int, int> {

        public class JoinOp {
            public bool isBlock { get; set; }
            public ActorRef op { get; set; }
        }

        private static int counter = 0;

        public static ActorRef getAddBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn) {

            var name = "block" + Guid.NewGuid().ToString();
            var head =  system.ActorOf(Props.Create( () => new HeadBlock() ), name);

            var speaker = system.ActorOf(Props.Create(() => new SpeakerActor(conn,name)), "speak" + counter++);
            head.Tell(new Messages.MarkHead { Stream = speaker });

            var log1 = system.ActorOf(Props.Create( () => new LogBlock() ), "log" + counter++);
            log1.Tell(new Messages.SetHead { Head = head });

            var add1 = system.ActorOf(Props.Create(() => new AddBlock(1) ), "add" + counter++);
            add1.Tell(new Messages.SetHead { Head = head });

            var log2 = system.ActorOf(Props.Create(() => new LogBlock()), "log" + counter++);
            log2.Tell(new Messages.SetHead { Head = head });

            head = joinOps(head, log1);
            joinOps(log1, add1);
            joinOps(add1, log2);
            head = endBlock(head, log2);

            return head;
        }

        public static ActorRef getComplexBlock(ActorSystem system, EventStore.ClientAPI.IEventStoreConnection conn) {

            var block1 = BlockFactory.getAddBlock(system, conn);
            var block2 = BlockFactory.getAddBlock(system, conn);
            var block3 = BlockFactory.getAddBlock(system, conn);

            return createBlock(new List<ActorRef> { block1, block2, block3 });
        }

        public static ActorRef createBlock(List<ActorRef> ops) {
            var head = ops.First();

            for (int i = 0, l = ops.Count; i < l; i++) { 
                var _next = i + 1 < ops.Count ? ops[i + 1] : null;
                if(_next != null){
                    _next.Tell(new Messages.SetHead { Head = head });
                    joinBlocks(ops[i],_next);
                }
            }
            
            return head;
        }


        public static ActorRef joinOps(ActorRef first, ActorRef second) {
            first.Tell(new Messages.SetNext { Next = second });
            return first;
        }

        public static ActorRef endBlock(ActorRef head, ActorRef tail) {
            tail.Tell(new Messages.SetNext { Next = head });
            tail.Tell(new Messages.MarkTail());

            return head;
        }

        public static ActorRef joinBlocks(ActorRef first, ActorRef second) {

            first.Tell(new Messages.SetNextBlock { NextBlock = second });

            return second;
        }
    }
}