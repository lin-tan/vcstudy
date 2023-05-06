package pfl.drivers;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Paths;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import io.grpc.stub.StreamObserver;
import pfl.Config;
import pfl.FatalLogHandler;
import pfl.MonitorContext;
import pfl.RPCLogProcessor;
import pfl.RPCLogServer;
import pfl.Utils;
import pfl.monitor.MsgSvcOuterClass.Log;
import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.monitor.BlockRpcSvcOuterClass.RPCsToBlock;
import pfl.shaded.com.google.protobuf.Empty;

public class AnalyzeDriver 
{
    public static void main(String[] args) throws Exception
    {
        MonitorContext ctx = new MonitorContext();
        ctx.RUNTIME_SERIALIZE = false;

        RPCLogServer logServer = new RPCLogServer();
        logServer.ctx = ctx;

        logServer.ctx.fatalLogHandler = new FatalLogHandler(logServer.ctx);
        logServer.fatalLogHandlerThread = new Thread(logServer.ctx.fatalLogHandler);
        logServer.fatalLogHandlerThread.start();

        logServer.ctx.rpcLogProcessor = new RPCLogProcessor(logServer.ctx);
        logServer.rpcLogProcessorThread = new Thread(logServer.ctx.rpcLogProcessor);
        logServer.rpcLogProcessorThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                logServer.ctx.isRunning = false;
            }
        }));
        System.out.println("LogServer started");

        // Offline Analysis
        
        // // Load RPC
        // File dir = new File(Config.RT_SERIALIZER_ROOTPATH);
        // FileFilter rpcFilter = new WildcardFileFilter("rpc_*");
        // ctx.rpcCounter = dir.listFiles(rpcFilter).length;
        // StreamObserver<RPCsToBlock> dummyObserver = new DummyStreamObserver<>();
        // for (int i = 0; i < ctx.rpcCounter; i++)
        // {
        //     String fn = "rpc_" + Integer.toString(i);
        //     Log log = Utils.deserializeProtobuf(Paths.get(Config.RT_SERIALIZER_ROOTPATH, fn), Log.parser());
        //     logServer.send(log, dummyObserver);
        // }

        // // Load System Fatal Log
        // FileFilter syslogFilter = new WildcardFileFilter("syslog_*");
        // ctx.syslogCounter = dir.listFiles(syslogFilter).length;
        // for (int i = 0; i < ctx.syslogCounter; i++)
        // {
        //     String fn = "syslog_" + Integer.toString(i);
        //     Log4jEvent event = Utils.deserializeProtobuf(Paths.get(Config.RT_SERIALIZER_ROOTPATH, fn), Log4jEvent.parser());
        //     logServer.sysFatalLog(event, dummyObserver);
        // }

        logServer.fatalLogHandlerThread.join();
        logServer.rpcLogProcessorThread.join();

    }
}

class DummyStreamObserver<T> implements StreamObserver<T>
{
    @Override
    public void onCompleted()
    {
        return;
    }

    @Override
    public void onError(Throwable arg0)
    {
        return;
    }

    @Override
    public void onNext(T arg0)
    {
        return;
    }
}
