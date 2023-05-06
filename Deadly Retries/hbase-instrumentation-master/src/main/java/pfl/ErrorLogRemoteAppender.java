package pfl;

import java.time.Instant;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import pfl.monitor.MsgSvcGrpc.MsgSvcBlockingStub;
import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.monitor.BlockRpcSvcOuterClass.RPCsToBlock;
import pfl.signatures.RPCBlockSignature;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class ErrorLogRemoteAppender extends AppenderSkeleton
{
    MsgSvcBlockingStub logStub;
    UUID nodeId;
    Set<RPCBlockSignature> blockedRpcSignature;

    public ErrorLogRemoteAppender(MsgSvcBlockingStub remoteStub, UUID nodeId, Set<RPCBlockSignature> blockedRpcSignature)
    {
        this.logStub = remoteStub;
        this.nodeId = nodeId;
        this.blockedRpcSignature = blockedRpcSignature;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean requiresLayout()
    {
        return false;
    }

    Function<LoggingEvent, Log4jEvent.Builder> getLog4jEventBuilderCommon = event ->
    {
        Log4jEvent.Builder builder = Log4jEvent.newBuilder();
        builder.setNodeId(nodeId.toString());
        Instant ts = Instant.ofEpochMilli(event.getTimeStamp());
        builder.setTimestamp(pfl.shaded.com.google.protobuf.Timestamp.newBuilder().setSeconds(ts.getEpochSecond()).setNanos(ts.getNano()).build());
        builder.setLevel(mapLogLevel(event.getLevel()));
        builder.setThreadName(event.getThreadName());
        builder.setClassName(event.getLocationInformation().getClassName());
        return builder;
    };

    @Override
    protected void append(LoggingEvent event)
    {
        // System.out.println("### " + event.getLevel() + ": " + event.getMessage());
        if (event.getLocationInformation().getClassName().contains("RpcServer"))
        {
            System.out.println("GET RPCSERVER");
            System.out.println(event.getMessage().toString());
            String msg = event.getMessage().toString();
            String[] split = msg.split(":");
            if (split.length == 2)
            {
                String[] s2 = split[0].split("/");
                if (s2.length == 3)
                {
                    String localBindAddr = s2[2];
                    Log4jEvent.Builder builder = getLog4jEventBuilderCommon.apply(event);
                    builder.setType(Log4jEvent.Type.RPC_ADDR);
                    builder.setMessage(localBindAddr);
                    logStub.sysFatalLog(builder.build());
                    return;
                }
            }
        }
        // System.out.println("### Received FATAL/ERROR 1");
        if (!event.getLevel().isGreaterOrEqual(org.apache.log4j.Level.ERROR)) return;
        // System.out.println("### Received FATAL/ERROR 2");
        Log4jEvent.Builder builder = getLog4jEventBuilderCommon.apply(event);
        builder.setType(Log4jEvent.Type.EVENT);
        builder.setMessage(event.getMessage().toString());
        logStub.sysFatalLog(builder.build());
        // System.out.println("### FATAL/ERROR send finish");
    }

    protected static Log4jEvent.Level mapLogLevel(org.apache.log4j.Level level)
    {
        switch (level.toInt())
        {
            case org.apache.log4j.Level.DEBUG_INT:
                return Log4jEvent.Level.DEBUG;
            case org.apache.log4j.Level.INFO_INT:
                return Log4jEvent.Level.INFO;
            case org.apache.log4j.Level.WARN_INT:
                return Log4jEvent.Level.WARN;
            case org.apache.log4j.Level.ERROR_INT:
                return Log4jEvent.Level.ERROR;
            case org.apache.log4j.Level.FATAL_INT:
                return Log4jEvent.Level.FATAL;
            case org.apache.log4j.Level.TRACE_INT:
                return Log4jEvent.Level.TRACE;
            default:
                return Log4jEvent.Level.OTHER;
        }
    }

}
