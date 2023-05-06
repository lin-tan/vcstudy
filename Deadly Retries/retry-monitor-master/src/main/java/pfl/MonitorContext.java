package pfl;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.grpc.Server;
import pfl.events.RPCCall;
import pfl.monitor.MsgSvcOuterClass.CallLog;
import pfl.monitor.MsgSvcOuterClass.ClientServerBindInfo;
import pfl.monitor.MsgSvcOuterClass.Log4jEvent;
import pfl.monitor.BlockRpcSvcGrpc.BlockRpcSvcBlockingStub;
import pfl.monitor.BlockRpcSvcOuterClass.RPCToBlock;
import pfl.shaded.com.google.common.collect.BiMap;
import pfl.shaded.com.google.common.collect.HashBiMap;
import pfl.shaded.com.google.common.collect.Maps;
import pfl.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

public class MonitorContext 
{
    public BlockingQueue<CallLog> tmpQueue = new LinkedBlockingQueue<>(); // RPC CallLog temp queue
    public Map<UUID, List<Log4jEvent>> systemFatalLogs = new ConcurrentHashMap<>(); // ID -> ERROR/FATAL logs
    public BiMap<UUID, String> ipAddrMap = Maps.synchronizedBiMap(HashBiMap.<UUID, String>create()); // UUID -> Bind IP:Port
    public Map<UUID, String> uuidNameMap = new ConcurrentHashMap<>(); // UUID -> MainClass
    public Map<UUID, SortedSet<RPCCall>> rpcLogs; // Receiver UUID -> [RPC Send Log]
    public BlockingQueue<ImmutablePair<UUID, List<RPCToBlock>>> rpcToBlock = new LinkedBlockingQueue<>(); // (Sender ID, [(Method, Param)])
    public Map<UUID, BlockRpcSvcBlockingStub> clientRpcStubs = new ConcurrentHashMap<>();
    public BlockingQueue<ClientServerBindInfo> pendingClientRpcConnection = new LinkedBlockingQueue<>();
    public BlockingQueue<ImmutablePair<UUID, Map<UUID, List<CallLog>>>> fatalRpcAnalysisQueue = new LinkedBlockingQueue<>(); // (Sender ID, Failed Node ID -> [RPC Logs])

    public FatalLogHandler fatalLogHandler;
    public Server server;
    public RPCLogProcessor rpcLogProcessor;

    public boolean isRunning = true;
    public boolean DEBUG = true;
    public boolean RPC_DEBUG = false;
    public boolean SYSLOG_DEBUG = true;
    public boolean RUNTIME_SERIALIZE = false;
    public Config config = new Config();
    public PrintWriter pw;

    public int rpcCounter = 0;
    public int syslogCounter = 0;

    public MonitorContext() throws FileNotFoundException
    {
        pw = new PrintWriter("./fatal.log");
    }
}
