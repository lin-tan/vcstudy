package pfl.events;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import pfl.monitor.MsgSvcOuterClass.CallLog;

/*
 * This class is used in two places: RPCLogPreprocessor and MonitorContext.rpcLogs.
 * 
 * RPCLogPreprocessor uses this class to match "Received RPC" and "Sent RPC". 
 * Since these two have the same (FromIP, ToIP, CallID, Method), they can be matched by this signature.
 * This class supports this match scheme by hashCode() and equals() method, which calculates the hash code
 * and the equals() by these four signatures.
 * rpcReceivePending in RPCLogPreprocessor is a ConcurrentHashMap, which leverages this hashing scheme to 
 * find matching "Received RPC" and "Sent RPC" calls.
 * 
 * The second objective of this function is to sort "Received RPC" calls by their timestamp, which is used 
 * in MonitorContext.rpcLogs. RPCLogPreProcessor separates received RPC calls by their receiver UUID, thus 
 * rpcLogs is a Map<UUID, SortedSet<RPCCall>>. The usage of SortedSet here is to ensure that the RPC calls
 * added are automatically sorted chronologically. 
 * This class supports the automated sorting requirement by the compareTo() methods, which works with 
 * ConcurrentSkipListSet to sort the calls automatically.
 * 
 * To avoid problems as a SortedSet (mainly the contains() method), the equals() method should be consistent
 * with compareTo(). Formally, the consistency is defined as "equals() == true" iff "compareTo() == 0".
 * Otherwise, we may be able to access an element with the iterator, but the contains() method still returns 
 * false. 
 * The compareTo() and equals() methods of RPCCall is consistent only under strict conditions.
 * RPCPreprocessor separates the received RPC calls according to their node ID. That is, each SortedSet in 
 * MonitorContext.rpcLogs only contains RPC calls received on one node, which indicates that they should be 
 * serialized. 
 * Under this scenario, if two RPC calls are "equal", they must be the same call, thus "compareTo() == 0", 
 * and vice versa.
 * To make the problem less severe, current implementation only iterates through the SortedSet without calling
 * the contains() method.
 * 
 */
public class RPCCall implements Comparable<RPCCall>
{
    public CallLog callLog;
    public UUID senderUuid;
    public UUID receiverUuid;

    public RPCCall(CallLog callLog)
    {
        this.callLog = callLog;
    }

    @Override
    public int compareTo(RPCCall rhs)
    {
        Instant lhsi = Instant.ofEpochSecond(this.callLog.getTimestamp().getSeconds(), this.callLog.getTimestamp().getNanos());
        Instant rhsi = Instant.ofEpochSecond(rhs.callLog.getTimestamp().getSeconds(), rhs.callLog.getTimestamp().getNanos());
        return lhsi.compareTo(rhsi);
    }

    @Override
    public int hashCode()
    {
        String fromIP = this.callLog.getRpcProperty().getFrom();
        String toIP = this.callLog.getRpcProperty().getTo();
        String id = this.callLog.getRpcProperty().getId();
        String method = this.callLog.getRpcProperty().getMethod();
        return Objects.hash(fromIP, toIP, id, method);
    }

    @Override
    public boolean equals(final Object obj) 
    {
        if (!(obj instanceof RPCCall)) return false;
        CallLog rhs = ((RPCCall) obj).callLog;
        CallLog lhs = this.callLog;
        return Objects.equals(lhs.getRpcProperty().getFrom(), rhs.getRpcProperty().getFrom()) 
            && Objects.equals(lhs.getRpcProperty().getTo(), rhs.getRpcProperty().getTo())
            && Objects.equals(lhs.getRpcProperty().getId(), rhs.getRpcProperty().getId())
            && Objects.equals(lhs.getRpcProperty().getMethod(), rhs.getRpcProperty().getMethod());
    }
}
