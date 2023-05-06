package pfl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import pfl.monitor.MsgSvcGrpc;
import pfl.monitor.BlockRpcSvcGrpc.BlockRpcSvcImplBase;
import pfl.monitor.MsgSvcGrpc.MsgSvcBlockingStub;
import pfl.monitor.MsgSvcOuterClass.CallLog;
import pfl.monitor.MsgSvcOuterClass.ClientServerBindInfo;
import pfl.monitor.MsgSvcOuterClass.Log;
import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.monitor.MsgSvcOuterClass.RPCMessageProperties;
import pfl.monitor.RpcParamsOuterClass.RpcParams;
import pfl.monitor.BlockRpcSvcOuterClass.RPCRequest;
import pfl.monitor.BlockRpcSvcOuterClass.RPCsToBlock;
import pfl.shaded.com.google.common.collect.EvictingQueue;
import pfl.shaded.com.google.common.collect.Queues;
import pfl.shaded.com.google.protobuf.ByteString;
import pfl.shaded.com.google.protobuf.Empty;
import pfl.shaded.com.google.protobuf.Timestamp;
import pfl.signatures.RPCBlockSignature;
import pfl.signatures.RpcParamUtils;

import org.apache.log4j.Logger;

@Aspect
public abstract class AbstractHBaseInstrument extends BlockRpcSvcImplBase
{
    private final MsgSvcBlockingStub logStub;
    Set<Long> grpcThreadMask = ConcurrentHashMap.newKeySet();
    Queue<CallLog> logQueue = Queues.synchronizedQueue(EvictingQueue.create(5000));
    UUID nodeId;
    boolean isShutdown = false;
    boolean useLocalLog = System.getProperty("pfl.monitor.endpoint") == null ? true : false;
    String monitorPos;
    String mainClass;
    ErrorLogRemoteAppender remoteAppender;
    boolean firstRecvSent = false;
    InstConfig instConfig = new InstConfig();
    
    Server localClientRpcServer;
    String localClientRpcServerBindAddr;
    int localClientRpcServerBindPort;
    BlockingQueue<RPCRequest> pendingRpcRequest = new LinkedBlockingQueue<>();

    Set<RPCBlockSignature> blockedRpcSignature = ConcurrentHashMap.newKeySet();

    boolean TRACE = false;

    public AbstractHBaseInstrument() throws Exception
    {
        System.out.println("### Instrumentation Loaded");
        nodeId = UUID.randomUUID();
        mainClass = Utils.getCurrentJvmMainClass();
        Runnable logThread;

        if (!useLocalLog) // Use remote logging
        {            
            monitorPos = System.getProperty("pfl.monitor.endpoint");

            // Local gRPC server listening for requests
            Socket tmpSock = new Socket(monitorPos, 44444);
            localClientRpcServerBindAddr = tmpSock.getLocalAddress().getHostAddress();
            tmpSock.close();
            localClientRpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(localClientRpcServerBindAddr, 0)).maxInboundMessageSize(2147483647).addService(this).build();
            localClientRpcServer.start();
            localClientRpcServerBindPort = localClientRpcServer.getPort();
            System.out.println("Local ClientRpcServer bind to: " + localClientRpcServerBindAddr + ":" + localClientRpcServerBindPort);

            System.out.println("### Sending logs to " + monitorPos);
            Channel channel = ManagedChannelBuilder.forAddress(monitorPos, 44444).usePlaintext().maxInboundMessageSize(2147483647).build();
            logStub = MsgSvcGrpc.newBlockingStub(channel);
            logStub.register(ClientServerBindInfo.newBuilder().setIp(localClientRpcServerBindAddr)
                .setPort(localClientRpcServerBindPort).setNodeId(nodeId.toString()).build());

            // Set RPC logger thread
            // logThread = new Runnable()
            // {
            //     @Override
            //     public void run()
            //     {
            //         try
            //         {
            //             long tid = Thread.currentThread().getId();
            //             grpcThreadMask.add(tid);
            //             boolean lastStep = false;
            //             while (!isShutdown || !lastStep)
            //             {
            //                 RPCRequest rpcRequest = pendingRpcRequest.take();
            //                 List<CallLog> tmpList = new ArrayList<>();
            //                 Instant targetTS = Utils.protoTimestampToInstant(rpcRequest.getTimestamp()); 
            //                 Duration targetGap = Utils.protoDurationToJavaDuration(rpcRequest.getDuration());
            //                 synchronized (logQueue)
            //                 {
            //                     while (!logQueue.isEmpty())
            //                     {   
            //                         CallLog clog = logQueue.poll();
            //                         if (!clog.getType().equals(CallLog.Type.RPC_SEND)) continue;
            //                         Instant logTS = Utils.protoTimestampToInstant(clog.getTimestamp());
            //                         Duration gap = Duration.between(logTS, targetTS); // Should be positive and less than targetGap, because we are looking for RPC calls before the crash
            //                         if ((!gap.isNegative()) && (gap.compareTo(targetGap) < 0))
            //                         {
            //                             tmpList.add(clog);
            //                         }
            //                     }
            //                 }
            //                 Log.Builder builder = Log.newBuilder();
            //                 builder.addAllCalls(tmpList);
            //                 builder.setNodeId(nodeId.toString());
            //                 logStub.send(builder.build());
            //                 if (isShutdown)
            //                     lastStep = true;
            //             };
            //         }
            //         catch (InterruptedException e)
            //         {
            //             e.printStackTrace();
            //         }
            //     }
            // };

            // Set root log appender
            remoteAppender = new ErrorLogRemoteAppender(logStub, nodeId, blockedRpcSignature);
            remoteAppender.setThreshold(org.apache.log4j.Level.INFO);
            remoteAppender.setName("FatalLogAppender");
            Logger.getRootLogger().addAppender(remoteAppender);
            // for (Enumeration appenders = Logger.getRootLogger().getAllAppenders(); appenders.hasMoreElements();)
            // {
            //     Appender appender = (Appender) appenders.nextElement();
            //     System.out.println("Appender: " + appender.getName());
            // }

            // Send main class to log server
            Log4jEvent.Builder builder = Log4jEvent.newBuilder();
            builder.setType(Log4jEvent.Type.MAIN_CLASS);
            builder.setNodeId(nodeId.toString());
            builder.setLevel(Log4jEvent.Level.OTHER);
            builder.setMessage(mainClass);
            logStub.sysFatalLog(builder.build());
        }
        else
        {
            monitorPos = System.getProperty("pfl.monitor.logPath", "/tmp");
            monitorPos += "/" + nodeId.toString() + "-" + mainClass + ".log";
            System.out.println("### Writng logs to " + monitorPos);
            logStub = null;
            logThread = new Runnable()
            {
                @Override
                public void run()
                {
                    long tid = Thread.currentThread().getId();
                    grpcThreadMask.add(tid);
                    boolean lastStep = false;
                    try (FileWriter fw = new FileWriter(monitorPos, true);
                            BufferedWriter bw = new BufferedWriter(fw);
                            PrintWriter pw = new PrintWriter(bw))
                    {
                        while (!isShutdown || !lastStep)
                        {
                            List<CallLog> tmpList = new ArrayList<>();
                            synchronized (logQueue)
                            {
                                while (!logQueue.isEmpty())
                                {
                                    tmpList.add(logQueue.poll());
                                }
                            }
                            for (CallLog clog : tmpList)
                            {
                                Instant ts = Instant.ofEpochSecond(clog.getTimestamp().getSeconds(), clog.getTimestamp().getNanos());
                                RPCMessageProperties rpcProperties = clog.getRpcProperty();
                                pw.println(String.format("%s:\t[%s (from: %s, to: %s)\tID: %s]\t%s", ts.toString(), rpcProperties.getDirection().name(), rpcProperties.getFrom(),
                                        rpcProperties.getTo(), rpcProperties.getId(), rpcProperties.getMethod()));
                                pw.println(new String(rpcProperties.getParam().toByteArray()));
                                pw.println();
                            }
                            pw.flush();
                            bw.flush();
                            fw.flush();
                            try
                            {
                                Thread.sleep(1000);
                            }
                            catch (InterruptedException e)
                            {
                                e.printStackTrace();
                            }
                            if (isShutdown)
                                lastStep = true;
                        }
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            };
            new Thread(logThread).start();
        }
    }

    // region BlockRpcSvc

    @Override
    public void requestRpcLog(RPCRequest request, StreamObserver<Log> responseObserver)
    {
        List<CallLog> tmpList;
        Instant targetTS = Utils.protoTimestampToInstant(request.getTimestamp()); 
        Duration targetGap = Utils.protoDurationToJavaDuration(request.getDuration());
        synchronized (logQueue)
        {
            tmpList = new ArrayList<>(logQueue);
        }
        List<CallLog> reply = new LinkedList<>();
        for (CallLog clog: tmpList)
        {
            if (!clog.getType().equals(CallLog.Type.RPC_SEND)) continue;
            Instant logTS = Utils.protoTimestampToInstant(clog.getTimestamp());
            Duration gap = Duration.between(logTS, targetTS); // Should be positive and less than targetGap, because we are looking for RPC calls before the crash
            if ((!gap.isNegative()) && (gap.compareTo(targetGap) < 0) && (request.getToIP().equals(clog.getRpcProperty().getTo())))
            //if ((!gap.isNegative()) && (gap.compareTo(targetGap) < 0))
            {
                reply.add(clog);
            }
        }
        Log.Builder builder = Log.newBuilder();
        builder.addAllCalls(reply);
        builder.setNodeId(nodeId.toString());
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

    }

    public void blockRpc(RPCsToBlock toBlock, StreamObserver<Empty> responseObserver) 
    {
        RPCBlockSignature.drainBlockedCallSignatures(toBlock, blockedRpcSignature);
        replyEmpty(responseObserver);
    }

    private void replyEmpty(StreamObserver<Empty> responseObserver)
    {
        Empty reply = Empty.getDefaultInstance();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    // endregion

    // region Util functions
    public CallLog.Builder trivialPCCommon(JoinPoint thisJoinPoint, long tid)
    {
        String callCtx;
        if (thisJoinPoint.getTarget() == null)
        {
            callCtx = "STATIC";
        }
        else
        {
            callCtx = thisJoinPoint.getTarget().getClass().toGenericString();
        }
        CallLog.Builder builder = CallLog.newBuilder();
        if (grpcThreadMask.contains(tid))
            return builder;

        builder.setType(CallLog.Type.NORMAL);
        builder.setTid(tid);
        builder.setPointcut(thisJoinPoint.toString());
        builder.setContext(callCtx);
        builder.setNodeId(nodeId.toString());
        Instant time = Instant.now();
        builder.setTimestamp(Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build());

        return builder;
    }

    Function<JoinPoint, Optional<CallLog.Builder>> getLogBuilderForFuncCall = thisJoinPoint ->
    {
        long threadID = Thread.currentThread().getId();
        if (grpcThreadMask.contains(threadID))
            return Optional.empty();
        CallLog.Builder loggingOnlyBuilder = trivialPCCommon(thisJoinPoint, threadID);
        return Optional.of(loggingOnlyBuilder);
    };

    Consumer<JoinPoint> trivialPointcut = thisJoinPoint ->
    {
        if (TRACE)
            System.out.println(thisJoinPoint);
        getLogBuilderForFuncCall.apply(thisJoinPoint).ifPresent(logBuilder -> logQueue.offer(logBuilder.build()));
    };
    // endregion

    // region RPC
    @Pointcut
    public void rpcSend()
    {
    }

    @Around("rpcSend()")
    public void rpcSend_monitor(ProceedingJoinPoint thisJoinPoint) throws Throwable
    {
        if (TRACE) System.out.println(thisJoinPoint);
        Optional<CallLog.Builder> optionalBuilder = getLogBuilderForFuncCall.apply(thisJoinPoint);
        if (!optionalBuilder.isPresent())
        {
            if (TRACE) System.out.println("Callbuilder is null. Returning...");
            thisJoinPoint.proceed();
            return;
        }
        CallLog.Builder logBuilder = optionalBuilder.get();
        logBuilder.setType(CallLog.Type.RPC_SEND);

        // Get from and to
        Object connectionObj = thisJoinPoint.getThis(); // RpcClient.Connection
        String fromIP, toIP;
        if (connectionObj == null)
        {
            if (TRACE) System.out.println("rpcServer is null. Returning...");
            thisJoinPoint.proceed();
            return;
        }
        Socket socketObj = (Socket) Utils.getObjectField(connectionObj, "socket");
        if (socketObj != null) // try "Socket"
        {
            fromIP = socketObj.getLocalAddress().getHostAddress() + ":" + Integer.toString(socketObj.getLocalPort()); // RPC Send side, so local address is the from
            toIP = socketObj.getInetAddress().getHostAddress() + ":" + Integer.toString(socketObj.getPort());
        }
        else // Try "Channel"
        {
            if (TRACE) System.out.println("socket is null. Trying io.netty.channel.Channel...");
            Object channelObj = Utils.getObjectField(connectionObj, "channel");
            if (channelObj == null)
            {
                if (TRACE) System.out.println("channel is also null. Give up and returning...");
                thisJoinPoint.proceed();
                return;
            }
            try
            {
                InetSocketAddress remoteAddr = (InetSocketAddress) Utils.invokeMethodNoArg(channelObj, "remoteAddress");
                toIP = remoteAddr.getAddress().getHostAddress() + ":" + Integer.toString(remoteAddr.getPort());
            }
            catch (Exception e)
            {
                if (TRACE)
                {
                    System.out.println("Exception in getting remote address from Channel. Returning...");
                    e.printStackTrace();
                }
                thisJoinPoint.proceed();
                return;
            }
            try
            {
                InetSocketAddress localAddr = (InetSocketAddress) Utils.invokeMethodNoArg(channelObj, "localAddress");
                fromIP = localAddr.getAddress().getHostAddress() + ":" + Integer.toString(localAddr.getPort());
            }
            catch (Exception e)
            {
                if (TRACE)
                {
                    System.out.println("Exception in getting local address from Channel. Returning...");
                    e.printStackTrace();
                }
                thisJoinPoint.proceed();
                return;
            }
        }

        // Get rpc call details
        Object callObj = thisJoinPoint.getArgs()[0];
        Integer callId = (Integer) Utils.getObjectField(callObj, "id");
        Object methodDescriptor = Utils.getObjectField(callObj, "md");
        String methodFullName = (String) Utils.invokeMethodNoArg(methodDescriptor, "getFullName");
        // if (Utils.isMaskedHBaseRPC(methodFullName)) 
        // {
        //     thisJoinPoint.proceed();
        //     return;
        // }
        Object paramObj = Utils.getObjectField(callObj, "param");
        RpcParams monitorRpcParams = RpcParamUtils.toMonitorRpcParams(methodFullName, paramObj);

        RPCMessageProperties.Builder rpcPropBuilder = RPCMessageProperties.newBuilder();
        rpcPropBuilder.setDirection(RPCMessageProperties.Direction.SEND);
        rpcPropBuilder.setId(callId.toString());
        rpcPropBuilder.setFrom(fromIP);
        rpcPropBuilder.setTo(toIP);
        rpcPropBuilder.setMethod(methodFullName);
        rpcPropBuilder.setParam(monitorRpcParams);

        logBuilder.setRpcProperty(rpcPropBuilder.build());
        logQueue.offer(logBuilder.build());

        // Check for blocked calls
        boolean isBlocked = RPCBlockSignature.fuzzyContains(new RPCBlockSignature(methodFullName, monitorRpcParams), blockedRpcSignature);
        if (!isBlocked)
        {
            if (TRACE) System.out.println("Proceed RPC call: " + new RPCBlockSignature(methodFullName, monitorRpcParams));
            thisJoinPoint.proceed();
        }
        else
        {
            System.out.println("Blocked RPC call: " + methodFullName);
        }

        if (TRACE) System.out.println("END: " + thisJoinPoint);
    }

    @Pointcut
    public void rpcReceive_processReq()
    {
    }

    @Pointcut
    public void rpcReceive()
    {
    }

    @Before("rpcReceive()")
    public void rpcReceive_monitor(JoinPoint thisJoinPoint) throws Exception
    {
        if ((firstRecvSent) && (!useLocalLog)) return;
        if (TRACE) System.out.println(thisJoinPoint);
        Optional<CallLog.Builder> optionalBuilder = getLogBuilderForFuncCall.apply(thisJoinPoint);
        if (!optionalBuilder.isPresent())
        {
            if (TRACE) System.out.println("Callbuilder is null. Returning...");
            return;
        }

        CallLog.Builder logBuilder = optionalBuilder.get();
        logBuilder.setType(CallLog.Type.RPC_RECEIVE);

        // Get from and to
        Object connectionObj = thisJoinPoint.getThis(); // cflow of RpcServer.Connection
        String fromIP, toIP;
        if (connectionObj == null)
        {
            if (TRACE) System.out.println("rpcServer is null. Returning...");
            return;
        }
        Socket socketObj = (Socket) Utils.getObjectField(connectionObj, "socket");
        if (socketObj != null) // try "Socket"
        {
            fromIP = socketObj.getInetAddress().getHostAddress() + ":" + Integer.toString(socketObj.getPort());
            toIP = socketObj.getLocalAddress().getHostAddress() + ":" + Integer.toString(socketObj.getLocalPort()); // This is RPC receive, so local == to
        }
        else // Try "Channel"
        {
            if (TRACE) System.out.println("socket is null. Trying io.netty.channel.Channel...");
            Object channelObj = Utils.getObjectField(connectionObj, "channel");
            if (channelObj == null)
            {
                if (TRACE) System.out.println("channel is also null. Give up and returning...");
                return;
            }
            try
            {
                InetSocketAddress remoteAddr = (InetSocketAddress) Utils.invokeMethodNoArg(channelObj, "remoteAddress");
                fromIP = remoteAddr.getAddress().getHostAddress() + ":" + Integer.toString(remoteAddr.getPort());
            }
            catch (Exception e)
            {
                if (TRACE)
                {
                    System.out.println("Exception in getting remote address from Channel. Returning...");
                    e.printStackTrace();
                }
                return;
            }
            try
            {
                InetSocketAddress localAddr = (InetSocketAddress) Utils.invokeMethodNoArg(channelObj, "localAddress");
                toIP = localAddr.getAddress().getHostAddress() + ":" + Integer.toString(localAddr.getPort());
            }
            catch (Exception e)
            {
                if (TRACE)
                {
                    System.out.println("Exception in getting local address from Channel. Returning...");
                    e.printStackTrace();
                }
                return;
            }
        }

        // Get rpc call details
        Integer callId = (Integer) thisJoinPoint.getArgs()[0 + instConfig.RECV_ARG_OFFSET]; // "+1" here for the constructor of RpcServer.Call, which is a nested class, and a pointer to the outer class is
                                                                   // passed as the first argument of the constructor
        Object methodDescriptor = thisJoinPoint.getArgs()[2 + instConfig.RECV_ARG_OFFSET];
        String methodFullName = (String) Utils.invokeMethodNoArg(methodDescriptor, "getFullName");
        // if (Utils.isMaskedHBaseRPC(methodFullName)) return;
        Object paramObj = thisJoinPoint.getArgs()[4 + instConfig.RECV_ARG_OFFSET];

        RPCMessageProperties.Builder rpcPropBuilder = RPCMessageProperties.newBuilder();
        rpcPropBuilder.setDirection(RPCMessageProperties.Direction.RECEIVE);
        rpcPropBuilder.setId(callId.toString());
        rpcPropBuilder.setFrom(fromIP);
        rpcPropBuilder.setTo(toIP);
        rpcPropBuilder.setMethod(methodFullName);
        rpcPropBuilder.setParam(RpcParamUtils.toMonitorRpcParams(methodFullName, paramObj));

        logBuilder.setRpcProperty(rpcPropBuilder.build());
        if ((!firstRecvSent) && (!useLocalLog))
        {
            Log.Builder builder = Log.newBuilder();
            builder.setNodeId(nodeId.toString());
            builder.addCalls(logBuilder.build());
            logStub.send(builder.build());
            firstRecvSent = true;
        }
        else
        {
            logQueue.offer(logBuilder.build());
        }
        if (TRACE) System.out.println("END: " + thisJoinPoint);
    }

    // endregion

    // @Pointcut("execution(* *.main(..))")
    // public void entrypoint()
    // {
    // }

    // @Before("entrypoint()")
    // public void entrypoint_monitor(JoinPoint thisJoinPoint)
    // {
    //     return;
    // }

}
