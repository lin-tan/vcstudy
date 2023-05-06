package pfl.signatures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pfl.DetectConfig;
import pfl.Utils;
import pfl.monitor.RpcParamsOuterClass.RepeatedParam;
import pfl.monitor.RpcParamsOuterClass.RpcParams;
import pfl.shaded.com.google.common.collect.Iterables;
import pfl.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

public class RpcParamUtils 
{
    public static Map<String, Integer> batchedCallDepth = new ConcurrentHashMap<>(); // Cache isBatchedCall tests
    
    // paramObj is a protobuf Message
    public static RpcParams toMonitorRpcParams(String methodName, Object paramObj) throws Exception 
    {
        RpcParams.Builder builder = RpcParams.newBuilder();
        ImmutablePair<Integer, Object> ret = getBatchLevel(methodName, paramObj);
        int batchLevel = ret.getLeft();
        if (batchLevel == 0)
        {
            builder.setBatchedCallDepth(batchLevel);
            builder.setNonBatchParam(paramObj.toString());
        }
        else
        {
            builder.setBatchedCallDepth(batchLevel);
            List<ImmutablePair<BatchLevelDescriptor, Object>> curLevelMsgFields = (List<ImmutablePair<BatchLevelDescriptor, Object>>) ret.getRight();
            Map<String, RepeatedParam.Builder> batchedParamsBuilder = new HashMap<>();
            Map<String, String> remainingParams = new HashMap<>();
            for (ImmutablePair<BatchLevelDescriptor, Object> field: curLevelMsgFields)
            {
                BatchLevelDescriptor desc = field.getLeft();
                Object value = field.getRight();
                String paramKey = desc.getFullDescriptorName();
                boolean isRepeated = desc.isRepeated();
                if (isRepeated)
                {
                    batchedParamsBuilder.computeIfAbsent(paramKey, k -> RepeatedParam.newBuilder()).addParams(value.toString());
                }
                else
                {
                    remainingParams.put(paramKey, value.toString());
                }
            }
            Map<String, RepeatedParam> batchedParams = new HashMap<>();
            batchedParamsBuilder.forEach((paramKey, rpBuilder) -> batchedParams.put(paramKey, rpBuilder.build()));
            builder.putAllBatchedParams(batchedParams);
            builder.putAllRemainingParams(remainingParams);
        }
        return builder.build();
    }

    public static ImmutablePair<Integer, Object> getBatchLevel(String methodName, Object paramObj) throws Exception
    {
        // If it is not a batched call
        if (batchedCallDepth.getOrDefault(methodName, -1) == 0) return ImmutablePair.of(0, paramObj);

        // Peeling initialization
        int level = 0;
        Object lastLevelParamObj = paramObj;
        Map<Object, Object> allMsgFields = (Map) Utils.invokeMethodNoArg(paramObj, "getAllFields");
        List<ImmutablePair<BatchLevelDescriptor, Object>> curLevelMsgFields = new ArrayList<>();
        for (Map.Entry<Object, Object> msg: allMsgFields.entrySet())
        {
            Object fieldDesc = msg.getKey();
            Object value = msg.getValue();
            curLevelMsgFields.add(ImmutablePair.of(new BatchLevelDescriptor(fieldDesc), value));
        }

        // If we have peeled this protobuf once, we can use the cached results instead of trying again
        if (batchedCallDepth.containsKey(methodName))
        {
            int targetLevel = batchedCallDepth.get(methodName);
            for (int i = 1; i < targetLevel; i++) // We use < instead of <= so that we can avoid the last useless call to peelMessage
            {
                lastLevelParamObj = curLevelMsgFields;
                if (i % 2 == 1)
                {
                    curLevelMsgFields = expandRepeated(curLevelMsgFields);
                }
                else
                {
                    curLevelMsgFields = peelMessage(curLevelMsgFields);
                }
            }
            return ImmutablePair.of(targetLevel, curLevelMsgFields);
        }

        // We peel the repeated fields level by level. In each level, if the string representation of repeated fields makes up of majority of the size, we consider that it is a batched call
        while (true)
        {   
            int repeatedFieldStringSize = 0;
            int nonRepeatedFieldStringSize = 0;
            for (ImmutablePair<BatchLevelDescriptor, Object> field: curLevelMsgFields)
            {
                BatchLevelDescriptor descriptors = field.getLeft();
                Object value = field.getRight();
                boolean isRepeated = descriptors.isRepeated();
                if (isRepeated)
                {
                    List params;
                    try
                    {
                        params = (List) value;
                    }
                    catch (Exception e)
                    {
                        if (descriptors.repeatedHasExpanded) repeatedFieldStringSize += value.toString().length();
                        continue;
                    }
                    for (Object param: params)
                    {
                        repeatedFieldStringSize += param.toString().length();
                    }
                }
                else
                {
                    nonRepeatedFieldStringSize += value.toString().length();
                }
            }
            // TODO: level >=4 here is hardcoded. Fix with herustics.
            if ((level >= 4) || ((double) repeatedFieldStringSize / (repeatedFieldStringSize + nonRepeatedFieldStringSize) < DetectConfig.BATCHED_CALL_THRESHOLD)) // Stop at current level and we don't need to go any deeper
            {
                // System.out.println("### Stop at level: " + level + " Ratio: " + ((double) repeatedFieldStringSize / (repeatedFieldStringSize + nonRepeatedFieldStringSize)));
                return ImmutablePair.of(level, lastLevelParamObj);
            }
            else // This is an interleaving process. In the initialization process, we have already peeled the message to fields (level 0). So in the level 1, we expand the repeated. 
            // Then we peel the message again in level 2. So on and so forth.
            {
                level++;
                lastLevelParamObj = curLevelMsgFields;
                if (level % 2 == 1)
                {
                    curLevelMsgFields = expandRepeated(curLevelMsgFields);
                }
                else
                {
                    curLevelMsgFields = peelMessage(curLevelMsgFields);
                }
                // TODO: We can also stop when expandedRepeated and peelMessage don't make any progress (i.e., make changes to the curLevelMsgFields)
            }
        }
    }

    private static List<ImmutablePair<BatchLevelDescriptor, Object>> expandRepeated(List<ImmutablePair<BatchLevelDescriptor, Object>> curLevelMsgFields) throws Exception
    {
        List<ImmutablePair<BatchLevelDescriptor, Object>> nextLevelMsgFields = new ArrayList<>();
        for (ImmutablePair<BatchLevelDescriptor, Object> field: curLevelMsgFields)
        {
            BatchLevelDescriptor currentLevelDescriptor = field.getLeft();
            Object currentLevelValue = field.getRight();
            boolean isRepeated = currentLevelDescriptor.isRepeated();
            if (isRepeated) // which also means that it must be a protobuf Message, so it would be safe to peel
            {
                // System.out.println("### At repeated param: " + currentLevelDescriptor.getFullDescriptorName());
                List params;
                try
                {
                    params = (List) currentLevelValue;
                }
                catch (Exception e)
                {
                    // System.out.println("### Failed to convert repeated field to list");
                    nextLevelMsgFields.add(field);
                    continue;
                }
                for (int i = 0; i < params.size(); i++)
                {
                    Object param = params.get(i);
                    nextLevelMsgFields.add(ImmutablePair.of(currentLevelDescriptor.deepCopyIncrement(), param));
                }
            }
            else // This is not a repeated message
            {
                // System.out.println("### At non-repeated param: " + currentLevelDescriptor.getFullDescriptorName());
                nextLevelMsgFields.add(field); 
            }
        }
        return nextLevelMsgFields;
    }

    private static List<ImmutablePair<BatchLevelDescriptor, Object>> peelMessage(List<ImmutablePair<BatchLevelDescriptor, Object>> curLevelMsgFields) throws Exception
    {
        List<ImmutablePair<BatchLevelDescriptor, Object>> nextLevelMsgFields = new ArrayList<>();
        for (ImmutablePair<BatchLevelDescriptor, Object> field: curLevelMsgFields)
        {
            BatchLevelDescriptor currentLevelDescriptor = field.getLeft();
            Object currentLevelValue = field.getRight();
            if (Utils.hasMethod(currentLevelValue, "getAllFields")) // This is an embedded Message, so we keep peeling it
            {
                Map<Object, Object> allFields = (Map) Utils.invokeMethodNoArg(currentLevelValue, "getAllFields");
                for (Map.Entry<Object, Object> onefield: allFields.entrySet())
                {
                    Object nextDescriptor = onefield.getKey();
                    Object nextValue = onefield.getValue();
                    nextLevelMsgFields.add(ImmutablePair.of(currentLevelDescriptor.deepCopyAppend(nextDescriptor, -1), nextValue));
                }
            }
            else // This may be a built-in type, we keep it as is.
            {
                nextLevelMsgFields.add(field); 
            }
        }
        return nextLevelMsgFields;
    }


    // public static boolean callIsBatched(String methodName, Object paramObj) throws Exception
    // {
    //     if (isBatchedCall.containsKey(methodName)) return isBatchedCall.get(methodName);
    //     Map allMsgFields = (Map) Utils.invokeMethodNoArg(paramObj, "getAllFields");
    //     int repeatedFieldStringSize = 0;
    //     int nonRepeatedFieldStringSize = 0;
    //     for (Object fd: allMsgFields.keySet())
    //     {
    //         Object rawParam = allMsgFields.get(fd);
    //         boolean isRepeated = (boolean) Utils.invokeMethodNoArg(fd, "isRepeated");
    //         if (isRepeated)
    //         {
    //             List params = (List) rawParam;
    //             for (Object param: params)
    //             {
    //                 repeatedFieldStringSize += param.toString().length();
    //             }
    //         }
    //         else
    //         {
    //             nonRepeatedFieldStringSize += rawParam.toString().length();
    //         }
    //     }
    //     if ((double) repeatedFieldStringSize / (repeatedFieldStringSize + nonRepeatedFieldStringSize) >= DetectConfig.BATCHED_CALL_THRESHOLD) // This is a batched call
    //     {
    //         isBatchedCall.put(methodName, true);
    //         return true;
    //     }
    //     else 
    //     {
    //         isBatchedCall.put(methodName, false);
    //         return false;
    //     }
    // }
}

class BatchLevelDescriptor
{
    public List<Object> descriptors;
    public List<Integer> repeatedIndex;
    public boolean repeatedHasExpanded; // False: PeelMessage; True: ExpandRepeated

    private BatchLevelDescriptor()
    {
    }

    public BatchLevelDescriptor(Object desc)
    {
        this.descriptors = new ArrayList<>(Arrays.asList(desc));
        this.repeatedIndex = new ArrayList<>(Arrays.asList(-1));
        this.repeatedHasExpanded = false;
    }

    public Object getLastDescriptor()
    {
        return Iterables.getLast(descriptors);
    }

    public boolean isRepeated() throws Exception
    {
        return (boolean) Utils.invokeMethodNoArg(this.getLastDescriptor(), "isRepeated");
    }

    public BatchLevelDescriptor deepCopyIncrement() // For expanding repeated messages (ExpandRepeated)
    {
        BatchLevelDescriptor newDescriptor = new BatchLevelDescriptor();
        newDescriptor.descriptors = new ArrayList<>(this.descriptors);
        newDescriptor.repeatedIndex = new ArrayList<>(this.repeatedIndex);
        newDescriptor.repeatedIndex.set(this.repeatedIndex.size() - 1, Iterables.getLast(this.repeatedIndex) + 1);
        newDescriptor.repeatedHasExpanded = true;
        return newDescriptor;
    }

    public BatchLevelDescriptor deepCopyAppend(Object nextDescriptor, int lastIndex) // For peeling nested Messages (PeelMessage)
    {
        BatchLevelDescriptor newDescriptor = new BatchLevelDescriptor();
        newDescriptor.descriptors = new ArrayList<>(this.descriptors);
        newDescriptor.descriptors.add(nextDescriptor);
        newDescriptor.repeatedIndex = new ArrayList<>(this.repeatedIndex);
        newDescriptor.repeatedIndex.add(lastIndex);
        newDescriptor.repeatedHasExpanded = false;
        return newDescriptor;
    }

    public String getFullDescriptorName() throws Exception
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Object desc: descriptors)
        {
            String name = (String) Utils.invokeMethodNoArg(desc, "getName");
            if (!isFirst) sb.append("."); else isFirst = false;
            sb.append(name);
        }
        return sb.toString();
    }
}
