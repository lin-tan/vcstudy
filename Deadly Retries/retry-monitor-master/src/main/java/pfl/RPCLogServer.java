package pfl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import pfl.monitor.MsgSvcGrpc.MsgSvcImplBase;
import pfl.monitor.MsgSvcOuterClass.ClientServerBindInfo;
import pfl.monitor.MsgSvcOuterClass.Log;
import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.monitor.BlockRpcSvcGrpc;
import pfl.monitor.BlockRpcSvcGrpc.BlockRpcSvcBlockingStub;
import pfl.monitor.BlockRpcSvcOuterClass.RPCToBlock;
import pfl.monitor.BlockRpcSvcOuterClass.RPCsToBlock;
import pfl.shaded.com.google.protobuf.Empty;
import pfl.shaded.com.google.protobuf.EmptyOrBuilder;
import pfl.shaded.com.google.protobuf.Message;
import pfl.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

public class RPCLogServer extends MsgSvcImplBase
{
    public MonitorContext ctx;
    public Thread fatalLogHandlerThread;
    public Thread rpcLogProcessorThread;
    public Thread clientRpcConnectorThread;
    public Thread rpcBlockerThread;

    public static void main(String[] args) throws Exception
    {
        // Start gRPC server
        RPCLogServer logServer = new RPCLogServer();
        logServer.ctx = new MonitorContext();
        logServer.ctx.server = NettyServerBuilder.forAddress(new InetSocketAddress("172.17.0.1", 44444)).maxInboundMessageSize(2147483647).addService(logServer).build();
        logServer.ctx.server.start();

        logServer.ctx.fatalLogHandler = new FatalLogHandler(logServer.ctx);
        logServer.fatalLogHandlerThread = new Thread(logServer.ctx.fatalLogHandler);
        logServer.fatalLogHandlerThread.start();

        logServer.ctx.rpcLogProcessor = new RPCLogProcessor(logServer.ctx);
        logServer.rpcLogProcessorThread = new Thread(logServer.ctx.rpcLogProcessor);
        logServer.rpcLogProcessorThread.start();

        logServer.clientRpcConnectorThread = new Thread(logServer.clientRpcConnector);
        logServer.clientRpcConnectorThread.start();

        logServer.rpcBlockerThread = new Thread(logServer.rpcBlocker);
        logServer.rpcBlockerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                logServer.ctx.isRunning = false;
                logServer.ctx.server.shutdown();
            }
        }));
        System.out.println("LogServer started");
        logServer.fatalLogHandlerThread.join();
        logServer.rpcLogProcessorThread.join();
        logServer.clientRpcConnectorThread.join();
        logServer.rpcBlockerThread.join();
    }

    // region gRPC implementaion

    @Override
    public void send(Log log, StreamObserver<Empty> responseObserver)
    {
        ctx.tmpQueue.addAll(log.getCallsList());
        serialize(log);
        replyEmpty(responseObserver);
    }

    @Override
    public void sysFatalLog(Log4jEvent event, StreamObserver<Empty> responseObserver)
    {
        if (ctx.DEBUG && ctx.SYSLOG_DEBUG) System.out.println("Sys: " + event.getMessage());
        switch (event.getTypeValue())
        {
            case Log4jEvent.Type.RPC_ADDR_VALUE:
            {
                String nodeId = event.getNodeId();
                String bindAddr = event.getMessage();
                ctx.ipAddrMap.put(UUID.fromString(nodeId), bindAddr);
                break;
            }
            case Log4jEvent.Type.MAIN_CLASS_VALUE:
            {
                String nodeId = event.getNodeId();
                String mainClass = event.getMessage();
                ctx.uuidNameMap.put(UUID.fromString(nodeId), mainClass);
                break;
            }
            case Log4jEvent.Type.EVENT_VALUE:
            {
                ctx.fatalLogHandler.addFatalLog(event.getNodeId(), event); // Add to FatalLogHandler to find repeated fatal events
                break;
            }
        }
        serialize(event);
        replyEmpty(responseObserver);
    }

    @Override
    public void register(ClientServerBindInfo bindInfo, StreamObserver<Empty> responseObserver)
    {
        ctx.pendingClientRpcConnection.offer(bindInfo);
        replyEmpty(responseObserver);
    }

    public void connectClientRpcServers() throws InterruptedException
    {
        while (ctx.isRunning)
        {
            ClientServerBindInfo bindInfo = ctx.pendingClientRpcConnection.take();
            try
            {
                Channel channel = ManagedChannelBuilder.forAddress(bindInfo.getIp(), bindInfo.getPort()).usePlaintext().maxInboundMessageSize(2147483647).build();
                BlockRpcSvcBlockingStub clientRpcStub = BlockRpcSvcGrpc.newBlockingStub(channel);
                UUID nodeId = UUID.fromString(bindInfo.getNodeId());
                ctx.clientRpcStubs.put(nodeId, clientRpcStub);
            }
            catch (Exception e)
            {
                ctx.pendingClientRpcConnection.put(bindInfo); // Unlimited Retry
            }
        }
    }

    Runnable clientRpcConnector = new Runnable() 
    {
        @Override
        public void run()
        {
            try 
            {
                connectClientRpcServers();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    };

    // endregion

    // region Send RPC Block Request
    public void sendRpcBlockRequest() throws InterruptedException
    {
        while (ctx.isRunning)
        {
            ImmutablePair<UUID, List<RPCToBlock>> pendingBlockReq = ctx.rpcToBlock.take();
            BlockRpcSvcBlockingStub stub = ctx.clientRpcStubs.get(pendingBlockReq.getLeft());
            RPCsToBlock.Builder rb = RPCsToBlock.newBuilder();
            rb.addAllRpcsToBlock(pendingBlockReq.getRight());
            try
            {
                stub.blockRpc(rb.build());
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    Runnable rpcBlocker = new Runnable() 
    {
        @Override
        public void run()
        {
            try
            {
                sendRpcBlockRequest();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    };

    // endregion

    private void replyEmpty(StreamObserver<Empty> responseObserver)
    {
        Empty reply = Empty.getDefaultInstance();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    // private void replyWithBlockReq(UUID callerUuid, StreamObserver<RPCsToBlock> responseObserver)
    // {
    //     RPCsToBlock.Builder toBlockReply = RPCsToBlock.newBuilder();
    //     List<RPCToBlock> tb = new ArrayList<>();
    //     ctx.rpcToBlock.computeIfAbsent(callerUuid, k -> new LinkedBlockingQueue<>()).drainTo(tb);
    //     toBlockReply.addAllRpcsToBlock(tb);
    //     responseObserver.onNext(toBlockReply.build());
    //     responseObserver.onCompleted();
    // }

    private <T extends Message> void serialize(T msg)
    {
        if (!ctx.RUNTIME_SERIALIZE) return;
        synchronized (ctx)
        {
            String fn = "fn";
            if (msg instanceof Log) // RPC
            {
                fn = "rpc_" + Integer.toString(ctx.rpcCounter);
                ctx.rpcCounter++;
            }
            else if (msg instanceof Log4jEvent)
            {
                fn = "syslog_" + Integer.toString(ctx.syslogCounter);
                ctx.syslogCounter++;
            }
            try
            {
                Utils.serializeProtobuf(msg, Paths.get(Config.RT_SERIALIZER_ROOTPATH, fn));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }
}
