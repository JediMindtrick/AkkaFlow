using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Akka;
using Akka.Actor;

namespace EventStoreClient
{
    public class OpsArgs {
        public int lh { get; set; }
        public int rh { get; set; }
        public int result { get; set; }
    }

    public class PipedActor<A,R> : ReceiveActor
    {
        protected Func<A, R> operation = null;
        protected ActorRef next = null;

        public PipedActor(Func<A,R> _operation, ActorRef _next) {

            operation = _operation;
            
            Receive<A>(arg => {
                var result = operation(arg);
                if (next != null) {
                    next.Tell(result);
                } 
            });
        }

        public PipedActor() : this(null,null) { }
    }

    public class Builder {

        private class OpDef{
            public string name { get ; set; }

            public dynamic p { get; set; }

        }

        private void newOp(string name, dynamic p = null) {
            this.ops.Enqueue(new OpDef { name = name, p = p });
        }

        private ActorSystem system = null;
        private Queue<OpDef> ops = new Queue<OpDef>();

        public ActorRef Built { get; private set; }

        public Builder(ActorSystem _system){
            system = _system;
            Built = null;
        }

        public static Builder create(ActorSystem _system) {
            return new Builder(_system);
        }

        public Builder begin() {
            newOp("begin");
            return this;
        }

        public Builder add(int howMany) {
            newOp("add", new { howMany = howMany });        
            return this;
        }

        public Builder log() {
            newOp("log");
            return this;
        }

        public Builder end() {
            newOp("end");
            return this;
        }

        public ActorRef build() {

            if (Built != null) return Built;

            Built = _build();
            return Built;
        }

        private ActorRef _build(){

            OpDef nextOp = _next();

            if (nextOp == null)
            {
                return Ops.nullVoid(system);
            }
            else {
                return _nonNull(nextOp, _build());
            }
        }

        private ActorRef _nonNull(OpDef op, ActorRef _next) {
            switch (op.name) { 
                case "begin":
                    return Ops.begin(system, _next);
                case "end":
                    return Ops.end(system, _next);
                case "log":
                    return Ops.logger(system, _next);
                case "add":
                    return Ops.adder(op.p.howMany, system, _next);
                default:
                    return Ops.nullVoid(system);
            }     
        }

        private OpDef _next() {
            if (ops.Count > 0)
            {
                return ops.Dequeue();
            }
            else {
                return null;
            }
        }
    }

    public class Ops {

        private static Func<OpsArgs, OpsArgs> add = arrgh => {

            Console.WriteLine("Adding {0} + {1}", arrgh.lh, arrgh.rh);

            return new OpsArgs { lh = arrgh.lh + arrgh.rh, rh = 0, result = arrgh.lh + arrgh.rh };
        };

        //yay, first-order-functions!!
        private static Func<int, Func<OpsArgs, OpsArgs>> genAdd = _in => {
            return args => {
                args.rh = _in;
                return add(args);
            };
        };

        public static ActorRef adder(int howMany, ActorSystem system, ActorRef next) {

            var toReturn = system.ActorOf(
                Props.Create(() => new PipedActor<OpsArgs, OpsArgs>(genAdd(howMany), next)), 
                Guid.NewGuid().ToString()
            );

            return toReturn;
        }

        private static Func<object, object> log = toLog =>
        {
            Console.WriteLine("Logging: " + JsonConvert.SerializeObject(toLog));

            return toLog;
        };

        public static ActorRef logger(ActorSystem system, ActorRef next) {

            var toReturn = system.ActorOf(
                Props.Create(() => new PipedActor<OpsArgs, object>(log, next)),
                Guid.NewGuid().ToString()
            );

            return toReturn;
        }

        private static Func<object, object> logBegin = toLog =>
        {
            Console.WriteLine("Begin Process! " + JsonConvert.SerializeObject(toLog));

            return toLog;
        };

        public static ActorRef begin(ActorSystem system, ActorRef next) {

            var toReturn = system.ActorOf(
                Props.Create(() => new PipedActor<OpsArgs, object>(logBegin, next)),
                Guid.NewGuid().ToString()
            );

            return toReturn;
        }

        private static Func<object, object> logEnd = toLog =>
        {
            Console.WriteLine("End Process! " + JsonConvert.SerializeObject(toLog));

            return toLog;
        };

        public static ActorRef end(ActorSystem system, ActorRef next)
        {

            var toReturn = system.ActorOf(
                Props.Create(() => new PipedActor<OpsArgs, object>(logEnd, next)),
                Guid.NewGuid().ToString()
            );

            return toReturn;
        }

        private static Func<object, object> logNull = toLog =>
        {
            Console.WriteLine("> null");

            return toLog;
        };

        public static ActorRef nullVoid(ActorSystem system) {
            var toReturn = system.ActorOf(
                Props.Create(() => new PipedActor<OpsArgs, object>(logNull, null)),
                Guid.NewGuid().ToString()
            );

            return toReturn;            
        }
    }
}
