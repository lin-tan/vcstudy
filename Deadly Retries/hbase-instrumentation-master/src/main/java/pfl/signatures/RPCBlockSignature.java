package pfl.signatures;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import pfl.DetectConfig;
import pfl.Utils;
import pfl.monitor.BlockRpcSvcOuterClass.RPCsToBlock;
import pfl.monitor.RpcParamsOuterClass.RpcParams;

public class RPCBlockSignature
{
    public String method;
    public RpcParams param;
    public Instant timeAdded;

    public RPCBlockSignature(String method, RpcParams param)
    {
        this.method = method;
        this.param = param;
        this.timeAdded = Instant.now();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(method, param);
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (!(obj instanceof RPCBlockSignature)) return false;
        RPCBlockSignature rhs = (RPCBlockSignature) obj;
        if (this.hashCode() != rhs.hashCode()) return false;
        return Objects.equals(this.method, rhs.method)
                && Objects.equals(this.param, rhs.param);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Method: " + method);
        sb.append('\n');
        sb.append("Param:\n");
        sb.append(param);
        return sb.toString();
    }

    public static void drainBlockedCallSignatures(RPCsToBlock rpcsToBlock, Set<RPCBlockSignature> blockedRpcSignature)
    {
        rpcsToBlock.getRpcsToBlockList().stream().forEach(c ->
        {
            RPCBlockSignature sig = new RPCBlockSignature(c.getMethod(), c.getParam());
            if (!blockedRpcSignature.add(sig)) // Update the addedTime
            {
                blockedRpcSignature.remove(sig);
                blockedRpcSignature.add(sig);
            }
        });
    }

    public static boolean fuzzyContains(RPCBlockSignature rpc, Set<RPCBlockSignature> blockedRpcSignature)
    {
        return blockedRpcSignature.stream().anyMatch(sig -> ((rpc.method.equals(sig.method)) && isParamSimilarFuzzy(rpc.param, sig.param)));
    }

    private static boolean isParamSimilarFuzzy(RpcParams toCompare, RpcParams sig)
    {
        if (toCompare.getBatchedCallDepth() != sig.getBatchedCallDepth()) return false;
        boolean isBatched = toCompare.getBatchedCallDepth() > 0;
        if (!isBatched)
        {
            return Utils.stringSimilarity(toCompare.getNonBatchParam(), sig.getNonBatchParam()) >= DetectConfig.RPC_BLOCK_PARAM_SIMILARITY_THRESHOLD;
        }
        else // If any of the block request exist in the batched call, we block the entire request. Also, we only consider the batched request at this period
        {
            for (String sigKey : sig.getBatchedParamsMap().keySet())
            {
                if (!toCompare.containsBatchedParams(sigKey)) continue;
                List<String> toCompareParams = toCompare.getBatchedParamsOrThrow(sigKey).getParamsList();
                boolean matched = sig.getBatchedParamsOrThrow(sigKey).getParamsList().stream().anyMatch(sigParam -> toCompareParams.stream()
                        .anyMatch(toCompareParam -> Utils.stringSimilarity(sigParam, toCompareParam) > DetectConfig.RPC_BLOCK_PARAM_SIMILARITY_THRESHOLD));
                if (matched) return true;
            }
            return false;
        }
    }
}
