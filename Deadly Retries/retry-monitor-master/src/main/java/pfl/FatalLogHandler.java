package pfl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

public class FatalLogHandler implements Runnable
{
    MonitorContext ctx;
    BlockingQueue<ImmutablePair<UUID, Log4jEvent>> fatalQueue; // (ID, Fatal Log)
    Random randomizer = new Random();

    public FatalLogHandler(MonitorContext ctx)
    {
        this.ctx = ctx;
        fatalQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run()
    {
        try
        {
            start();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void addFatalLog(String id, Log4jEvent e)
    {
        UUID uuid = UUID.fromString(id); // Receiver ID
        fatalQueue.offer(ImmutablePair.of(uuid, e));
    }

    public void start() throws InterruptedException
    {
        while (ctx.isRunning)
        {
            List<ImmutablePair<UUID, Log4jEvent>> receivedFatalEvents = new ArrayList<>();
            receivedFatalEvents.add(fatalQueue.take());
            fatalQueue.drainTo(receivedFatalEvents);
            ImmutablePair<UUID, Log4jEvent> entry = receivedFatalEvents.get(randomizer.nextInt(receivedFatalEvents.size())); // Randomly pick one and search for repeated events
            receivedFatalEvents.forEach(e -> ctx.systemFatalLogs.computeIfAbsent(e.getLeft(), k -> new LinkedList<>()).add(e.getRight())); // Add to systemFatalLogs
            List<ImmutablePair<UUID, Log4jEvent>> similarFatalEvents = new LinkedList<>();
            similarFatalEvents.add(entry);
            for (UUID id: ctx.systemFatalLogs.keySet())
            {
                if (id.equals(entry.getLeft())) continue; // We want fatal event that happen on different nodes
                for (Log4jEvent flog: ctx.systemFatalLogs.get(id))
                {
                    // if (ctx.DEBUG)
                    // {
                    //     ctx.pw.println("FatalLogHandler: Similarity score: " + Utils.stringSimilarity(flog.getMessage(), entry.getRight().getMessage()));
                    //     ctx.pw.flush();
                    // }
                    if (Utils.stringSimilarity(flog.getMessage(), entry.getRight().getMessage()) >= ctx.config.SYSLOG_SIMILARITY_THRESHOLD)
                    {
                        similarFatalEvents.add(ImmutablePair.of(id, flog));
                        // if (ctx.DEBUG)
                        // {
                        //     ctx.pw.println("FatalLogHandler: Added repeated fatal on " + id + " | " + ctx.uuidNameMap.get(id));
                        //     ctx.pw.println(flog.getMessage());
                        //     ctx.pw.flush();
                        // }
                    }
                }
            }
            if (similarFatalEvents.size() > 1) ctx.rpcLogProcessor.getRepeatedSystemFatalEvents(similarFatalEvents); // Send to RPCLogProcessor to find relevant RPC calls
            Thread.sleep(randomizer.nextInt(200)); // Throttle the request so we can get a chance of batching. 
                                                // Each repeated system fatal event we sent here will trigger a round of on-demand RPC request at the RPCLogProcessor side
                                                // TODO: Adapt the wait time w.r.t. to pending elements in RPCLogProcessor.repeatedSystemFatalQueue?
        }
    }
}

// Ver.2

// while (ctx.isRunning)
// {
//     List<ImmutablePair<UUID, Log4jEvent>> receivedFatalEvents = new ArrayList<>();
//     receivedFatalEvents.add(fatalQueue.take());
//     fatalQueue.drainTo(receivedFatalEvents);
//     ImmutablePair<UUID, Log4jEvent> entry = receivedFatalEvents.get(randomizer.nextInt(receivedFatalEvents.size())); // Randomly pick one and search for repeated events
//     receivedFatalEvents.forEach(e -> ctx.systemFatalLogs.computeIfAbsent(e.getLeft(), k -> new LinkedList<>()).add(e.getRight())); // Add to systemFatalLogs
//     List<ImmutablePair<UUID, Log4jEvent>> similarFatalEvents = new LinkedList<>();
//     similarFatalEvents.add(entry);
//     for (UUID id: ctx.systemFatalLogs.keySet())
//     {
//         if (id.equals(entry.getLeft())) continue; // We want fatal event that happen on different nodes
//         for (Log4jEvent flog: ctx.systemFatalLogs.get(id))
//         {
//             if (Utils.stringSimilarity(flog.getMessage(), entry.getRight().getMessage()) >= ctx.config.SYSLOG_SIMILARITY_THRESHOLD)
//             {
//                 similarFatalEvents.add(ImmutablePair.of(id, flog));
//             }
//         }
//     }
//     if (similarFatalEvents.size() > 1) ctx.rpcLogProcessor.getRepeatedSystemFatalEvents(similarFatalEvents); // Send to RPCLogProcessor to find relevant RPC calls
//     Thread.sleep(randomizer.nextInt(200)); // Throttle the request so we can get a chance of batching. 
//                                            // Each repeated system fatal event we sent here will trigger a round of on-demand RPC request at the RPCLogProcessor side
//                                            // TODO: Adapt the wait time w.r.t. to pending elements in RPCLogProcessor.repeatedSystemFatalQueue?
// }

// Ver.1

// while (ctx.isRunning)
// {
//     ImmutablePair<UUID, Log4jEvent> entry = fatalQueue.take();
//     ctx.systemFatalLogs.computeIfAbsent(entry.getLeft(), k -> new LinkedList<>()).add(entry.getRight());
//     boolean alerted = false;
//     for (UUID id: ctx.systemFatalLogs.keySet())
//     {
//         if (id.equals(entry.getLeft())) continue;
//         for (Log4jEvent flog: ctx.systemFatalLogs.get(id))
//         {
//             if (Utils.stringSimilarity(flog.getMessage(), entry.getRight().getMessage()) >= ctx.config.SYSLOG_SIMILARITY_THRESHOLD)
//             {
//                 List<ImmutablePair<UUID, Log4jEvent>> tmp = new LinkedList<>();
//                 tmp.add(entry);
//                 tmp.add(ImmutablePair.of(id, flog));
//                 ctx.rpcLogProcessor.getRepeatedSystemFatalEvents(tmp); // Send to RPCLogProcessor to find relevant RPC calls
//                 alerted = true;
//                 break;
//             }
//         }
//         if (alerted) break;
//     }
// }