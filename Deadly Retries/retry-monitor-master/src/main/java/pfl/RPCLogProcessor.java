package pfl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

import pfl.events.RPCCall;
import pfl.monitor.MsgSvcOuterClass.CallLog;
import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.monitor.RpcParamsOuterClass.RepeatedParam;
import pfl.monitor.RpcParamsOuterClass.RpcParams;
import pfl.monitor.BlockRpcSvcGrpc.BlockRpcSvcBlockingStub;
import pfl.monitor.BlockRpcSvcOuterClass.RPCRequest;
import pfl.monitor.BlockRpcSvcOuterClass.RPCToBlock;
import pfl.monitor.BlockRpcSvcOuterClass.RPCsToBlock;
import pfl.shaded.com.google.protobuf.ByteString;
import pfl.shaded.org.apache.commons.lang3.tuple.ImmutablePair;
import pfl.shaded.org.apache.commons.lang3.tuple.ImmutableTriple;
import pfl.shaded.org.apache.commons.lang3.tuple.MutablePair;

public class RPCLogProcessor implements Runnable
{
    MonitorContext ctx;
    BlockingQueue<List<ImmutablePair<UUID, Log4jEvent>>> repeatedSystemFatalQueue;
    int fatalCount = 0;

    public RPCLogProcessor(MonitorContext ctx) throws FileNotFoundException
    {
        this.ctx = ctx;
        this.repeatedSystemFatalQueue = new LinkedBlockingQueue<>();
        this.ctx.rpcLogs = new ConcurrentHashMap<>();
    }

    @Override
    public void run()
    {
        Thread preprocessor = new Thread(new RPCPreProcessor(ctx));
        preprocessor.start();
        Thread rpcLogRequestorThread = new Thread(this.rpcLogRequestor);
        rpcLogRequestorThread.start();
        try
        {
            this.startAnalyzer();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    // region Analyze fatal rpc logs can block them

    // Core logic
    public void startAnalyzer() throws InterruptedException, IOException
    {
        while (ctx.isRunning)
        {
            ImmutablePair<UUID, Map<UUID, List<CallLog>>> analyzerRequest = ctx.fatalRpcAnalysisQueue.take(); // (Sender ID, Receiver ID -> [RPC Logs]), rpc logs from [T - FATAL_THRESHOLD, T]
            UUID senderUuid = analyzerRequest.getLeft();
            ctx.pw.println("RPCLogProcessor: Potential Buggy Caller: " + senderUuid + " | " + ctx.uuidNameMap.get(senderUuid));
            ctx.pw.flush();
            // Group the RPC calls by Method Name
            Map<String, List<ImmutablePair<UUID, CallLog>>> potentialDeadlyRpc = new HashMap<>(); // Method -> [(Receiver UUID, CallLog)]
            for (UUID receiverId: analyzerRequest.getRight().keySet())
            {
                for (CallLog clog: analyzerRequest.getRight().get(receiverId))
                {
                    String methodName = clog.getRpcProperty().getMethod();
                    potentialDeadlyRpc.computeIfAbsent(methodName, k -> new ArrayList<>()).add(ImmutablePair.of(receiverId, clog));
                }
            }

            // Check whether the repeated RPC call is a retried call
            for (String methodName: potentialDeadlyRpc.keySet()) 
            {
                List<ImmutablePair<UUID, CallLog>> calls = potentialDeadlyRpc.get(methodName); // [(Receiver UUID, CallLog)]
                if (ctx.DEBUG) 
                {
                    ctx.pw.println("Checking method: " + methodName + " size: " + calls.size());
                    ctx.pw.flush();
                }
                if (calls.size() < 2) continue;
                for (int i = 0; i < calls.size() - 1; i++)
                {
                    for (int j = i + 1; j < calls.size(); j++)
                    {
                        if (calls.get(i).getLeft().equals(calls.get(j).getLeft())) continue; // We need retried calls on different receiver
                        CallLog call1 = calls.get(i).getRight();
                        CallLog call2 = calls.get(j).getRight();
                        RpcParams param1 = call1.getRpcProperty().getParam();
                        RpcParams param2 = call2.getRpcProperty().getParam();
                        ImmutableTriple<Boolean, RpcParams, RpcParams> similarityCheckResult = isDeadlyRetry(param1, param2); // (IsRetry, Signature 1, Signature 2)
                        if (similarityCheckResult.getLeft())
                        {
                            List<RPCToBlock> toBlock = new ArrayList<>();
                            toBlock.add(RPCToBlock.newBuilder().setMethod(methodName).setParam(similarityCheckResult.getMiddle()).build());
                            toBlock.add(RPCToBlock.newBuilder().setMethod(methodName).setParam(similarityCheckResult.getRight()).build());
                            blockRPC(senderUuid, toBlock);

                            ctx.pw.println("---------- Buggy Call 1 -------------");
                            ctx.pw.println("From: " + call1.getRpcProperty().getFrom() + " | " + ctx.uuidNameMap.get(senderUuid));
                            ctx.pw.println("To: " + call1.getRpcProperty().getTo() + " | " + ctx.uuidNameMap.get(calls.get(i).getLeft()));
                            ctx.pw.println("Call ID: " + call1.getRpcProperty().getId());
                            ctx.pw.println("Timestamp: " + Utils.protoBufTStoInstant(call1.getTimestamp()));
                            ctx.pw.println("Method: " + call1.getRpcProperty().getMethod());
                            ctx.pw.println(similarityCheckResult.getMiddle());
                            ctx.pw.println("---------- Buggy Call 2 -------------");
                            ctx.pw.println("From: " + call2.getRpcProperty().getFrom() + " | " + ctx.uuidNameMap.get(senderUuid));
                            ctx.pw.println("To: " + call2.getRpcProperty().getTo() + " | " + ctx.uuidNameMap.get(calls.get(j).getLeft()));
                            ctx.pw.println("Call ID: " + call2.getRpcProperty().getId());
                            ctx.pw.println("Timestamp: " + Utils.protoBufTStoInstant(call2.getTimestamp()));
                            ctx.pw.println("Method: " + call2.getRpcProperty().getMethod());
                            ctx.pw.println(similarityCheckResult.getRight());
                            ctx.pw.println("-----------------------------------");
                            ctx.pw.flush();
                        }
                        else
                        {
                            if (ctx.DEBUG)
                            {
                                ctx.pw.println("Call to " + call1.getRpcProperty().getTo() + " and " + call2.getRpcProperty().getTo() + " is NOT retried");
                                ctx.pw.println("Param Pattern 1:");
                                ctx.pw.println(similarityCheckResult.getMiddle());
                                ctx.pw.println("Param Pattern 2:");
                                ctx.pw.println(similarityCheckResult.getRight());
                                ctx.pw.flush();
                            }
                        }
                    }
                }
            }

        }
    }

    private ImmutableTriple<Boolean, RpcParams, RpcParams> isDeadlyRetry(RpcParams param1, RpcParams param2) // Returns <IsDeadlyRetry, Signature1 ToBlock, Signature2 ToBlock>
    {
        if (param1.getBatchedCallDepth() != param2.getBatchedCallDepth()) return ImmutableTriple.of(false, null, null); // It should not happen, as we group the calls by methodName
        boolean isBatchedCall = param1.getBatchedCallDepth() > 0;
        if (!isBatchedCall)
        {
            String param1Str = param1.getNonBatchParam();
            String param2Str = param2.getNonBatchParam();
            boolean isRetry = Utils.stringSimilarity(param1Str, param2Str) >= ctx.config.RPC_PARAM_SIMILARITY_THRESHOLD;
            return ImmutableTriple.of(isRetry, param1, param2);
        }
        else
        {
            RpcParams.Builder sig1Builder = RpcParams.newBuilder().setBatchedCallDepth(param1.getBatchedCallDepth());
            RpcParams.Builder sig2Builder = RpcParams.newBuilder().setBatchedCallDepth(param2.getBatchedCallDepth());
            boolean hasRetriedCall = false;
            for (String sigKey: param1.getBatchedParamsMap().keySet())
            {
                if (!param2.containsBatchedParams(sigKey)) continue;
                if (ctx.DEBUG) ctx.pw.println("Get pattern for: " + sigKey + "Param1 Count: " + param1.getBatchedParamsOrThrow(sigKey).getParamsList().size() + " Param2 Count: " + param2.getBatchedParamsOrThrow(sigKey).getParamsList().size());
                List<String> param1Patterns = getBatchedParamPatterns(param1.getBatchedParamsOrThrow(sigKey).getParamsList());
                List<String> param2Patterns = getBatchedParamPatterns(param2.getBatchedParamsOrThrow(sigKey).getParamsList());
                if (ctx.DEBUG) ctx.pw.flush();

                // Compare patterns from param1 and param2, find retried calls
                boolean thisKeyRetried = false;
                boolean[] param2Visited = new boolean[param2Patterns.size()];
                Arrays.fill(param2Visited, false);
                RepeatedParam.Builder rp1Builder = RepeatedParam.newBuilder();
                RepeatedParam.Builder rp2Builder = RepeatedParam.newBuilder();
                for (String param1Pattern: param1Patterns)
                {
                    boolean p1Retried = false;
                    for (int i = 0; i < param2Patterns.size(); i++)
                    {
                        if (param2Visited[i]) continue;
                        String param2Pattern = param2Patterns.get(i);
                        if (Utils.stringSimilarity(param1Pattern, param2Pattern) >= ctx.config.RPC_PARAM_SIMILARITY_THRESHOLD)
                        {
                            if (!p1Retried) rp2Builder.addParams(param2Pattern);
                            param2Visited[i] = true;
                            p1Retried = true;
                        }
                    }
                    if (p1Retried) 
                    {
                        rp1Builder.addParams(param1Pattern);
                        thisKeyRetried = true;
                    }
                }
                if (thisKeyRetried)
                {
                    sig1Builder.putBatchedParams(sigKey, rp1Builder.build());
                    sig2Builder.putBatchedParams(sigKey, rp2Builder.build());
                    hasRetriedCall = true;
                }
            }
            return ImmutableTriple.of(hasRetriedCall, sig1Builder.build(), sig2Builder.build());
        }
    }

    private List<String> getBatchedParamPatterns(List<String> params)
    {
        List<String> paramPatterns = new ArrayList<>();
        boolean[] visited = new boolean[params.size()];
        Arrays.fill(visited, false);
        for (int i = 0; i < params.size(); i++)
        {
            if (visited[i]) continue;
            visited[i] = true;
            String currentPattern = params.get(i);
            if (ctx.DEBUG)
            {
                ctx.pw.println("Pattern " + i + " : " + currentPattern);
            }
            for (int j = i + 1; j < params.size(); j++)
            {
                if (visited[j]) continue;
                if (Utils.stringSimilarity(currentPattern, params.get(j)) >= ctx.config.RPC_PARAM_SIMILARITY_THRESHOLD) 
                {
                    visited[j] = true;
                }

            }
            paramPatterns.add(currentPattern);
        }
        return paramPatterns;
    }

    public void blockRPC(UUID senderUuid, List<RPCToBlock> rpcToBlock) throws InterruptedException
    {
        ctx.rpcToBlock.put(ImmutablePair.of(senderUuid, rpcToBlock));
    }

    // endregion

    public void getRepeatedSystemFatalEvents(List<ImmutablePair<UUID, Log4jEvent>> e)
    {
        repeatedSystemFatalQueue.offer(e);
    }

    // region On-demand request for RPC logs
    public void requestRpcLogs() throws InterruptedException
    {
        while (ctx.isRunning)
        {
            List<ImmutablePair<UUID, Log4jEvent>> fatalEvents = repeatedSystemFatalQueue.take(); // UUID here is Receiver ID
            fatalCount++;
            ctx.pw.println("RPCLogProcessor: ========= Fatal " + fatalCount + " ==========");
            ctx.pw.flush();
            // for (ImmutablePair<UUID, Log4jEvent> e: fatalEvents)
            // {
            //     ctx.pw.println(e.getLeft() + ": " + ctx.uuidNameMap.get(e.getLeft()));
            //     ctx.pw.println(Utils.protoBufTStoInstant(e.getRight().getTimestamp()));
            //     ctx.pw.println(e.getRight().getMessage());
            //     ctx.pw.flush();
            // }
            Duration targetGap = Duration.ofMillis(ctx.config.FATAL_THRESHOLD);
            for (UUID senderId: ctx.clientRpcStubs.keySet())
            {
                BlockRpcSvcBlockingStub stub = ctx.clientRpcStubs.get(senderId);
                Map<UUID, List<CallLog>> potentialDeadlyRpcLog = new HashMap<>(); // ReceiverID -> [RPC Log]
                boolean gotPotentialRpc = false;
                for (ImmutablePair<UUID, Log4jEvent> fatalEvent: fatalEvents) // (ID where the fatal log happens (ReceiverID), log)
                {
                    RPCRequest.Builder rBuilder = RPCRequest.newBuilder();
                    rBuilder.setTimestamp(fatalEvent.getRight().getTimestamp()).setDuration(Utils.javaDurationToProtoDuration(targetGap)).setToIP(ctx.ipAddrMap.get(fatalEvent.getLeft()));
                    RPCRequest request = rBuilder.build();
                    // if (ctx.DEBUG)
                    // {
                    //     ctx.pw.println("RPCLogProcessor: Requesting RPC log from: " + ctx.uuidNameMap.get(senderId) + " To: " + request.getToIP());
                    // }
                    try
                    {
                        List<CallLog> callLogs = stub.requestRpcLog(request).getCallsList();
                        if (callLogs.size() > 0) 
                        {
                            gotPotentialRpc = true;
                            // if (ctx.DEBUG)
                            // {
                            //     ctx.pw.println("Got " + callLogs.size() + " RPC logs from: " + ctx.uuidNameMap.get(senderId));
                            // }
                        }
                        potentialDeadlyRpcLog.computeIfAbsent(fatalEvent.getLeft(), k -> new ArrayList<>()).addAll(callLogs);
                    }
                    catch (Exception e)
                    {
                        if (ctx.DEBUG) System.out.println("RPC Request to " + ctx.uuidNameMap.get(senderId) + " has failed. Reason: " + e.toString());
                        break; // the stub has failed, meaning that the node must have crashed and we don't need to consider this node anymore
                    }
                }
                if (gotPotentialRpc) // Send for analysis
                {
                    // ctx.pw.println("RPCLogProcessor: Got potential deadly RPC logs. Sender: " + ctx.uuidNameMap.get(senderId));
                    // ctx.pw.flush();
                    ctx.fatalRpcAnalysisQueue.put(ImmutablePair.of(senderId, potentialDeadlyRpcLog));
                }   
            }
        }
    }

    Runnable rpcLogRequestor = new Runnable() 
    {
        @Override
        public void run()
        {
            try
            {
                requestRpcLogs();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    };

    // endregion

}

class RPCPreProcessor implements Runnable
{
    MonitorContext ctx;
    Map<RPCCall, RPCCall> rpcReceivePending = new ConcurrentHashMap<>(); // Received RPC -> Received RPC, but can also be indexed by the Sent RPC.
                                                                         // See comments in RPCCall.java for more details.
    BlockingQueue<CallLog> rpcSendQueue = new LinkedBlockingQueue<>();

    public RPCPreProcessor(MonitorContext ctx)
    {
        this.ctx = ctx;
    }

    @Override
    public void run()
    {
        Thread senderMatcher = new Thread(new Runnable() {
            @Override
            public void run()
            {
                try
                {
                    while (ctx.isRunning)
                    {
                        CallLog t = rpcSendQueue.take();
                        String toIP = t.getRpcProperty().getTo();
                        if (!ctx.ipAddrMap.containsValue(toIP))
                        {
                            rpcSendQueue.put(t);
                            continue;
                        }
                        RPCCall logEntry = new RPCCall(t);
                        logEntry.senderUuid = UUID.fromString(t.getNodeId());
                        logEntry.receiverUuid = ctx.ipAddrMap.inverse().get(toIP);
                        ctx.rpcLogs.computeIfAbsent(logEntry.receiverUuid, k -> new ConcurrentSkipListSet<>()).add(logEntry);
                        // RPCCall logEntry = new RPCCall(t);
                        // if (rpcReceivePending.containsKey(logEntry))
                        // {
                        //     RPCCall receiveLog = rpcReceivePending.get(logEntry);
                        //     UUID receiverUuid = UUID.fromString(receiveLog.callLog.getNodeId());
                        //     receiveLog.senderUuid = UUID.fromString(t.getNodeId());
                        //     ctx.rpcLogs.computeIfAbsent(receiverUuid, k -> new ConcurrentSkipListSet<>()).add(receiveLog);
                        //     rpcReceivePending.remove(receiveLog);
                        // }
                        // else
                        // {
                        //     rpcSendQueue.put(t);
                        // }
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        });
        senderMatcher.start();

        try
        {
            while (ctx.isRunning)
            {
                CallLog t = ctx.tmpQueue.take();
                RPCCall logEntry = new RPCCall(t);
                switch (t.getTypeValue())
                {
                case CallLog.Type.RPC_SEND_VALUE:
                    rpcSendQueue.put(t);
                    break;
                case CallLog.Type.RPC_RECEIVE_VALUE:
                    ctx.ipAddrMap.putIfAbsent(UUID.fromString(t.getNodeId()), t.getRpcProperty().getTo());
                    rpcReceivePending.put(logEntry, logEntry);
                    break;
                }

                if (ctx.DEBUG && ctx.RPC_DEBUG) System.out.println("RPC: " + t.getType() + " From: " + t.getRpcProperty().getFrom() + " To: " + t.getRpcProperty().getTo());
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
}