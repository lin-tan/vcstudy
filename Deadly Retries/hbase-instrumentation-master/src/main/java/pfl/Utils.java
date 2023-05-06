package pfl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import pfl.shaded.org.reflections.ReflectionUtils;
import pfl.signatures.RPCBlockSignature;
import pfl.shaded.com.google.common.io.ByteStreams;
import pfl.shaded.com.google.common.reflect.TypeToken;
import pfl.shaded.com.google.protobuf.Timestamp;
import pfl.shaded.org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import pfl.shaded.org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import pfl.shaded.org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import pfl.shaded.org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import pfl.shaded.org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import pfl.shaded.org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import pfl.shaded.org.apache.commons.text.similarity.LevenshteinDistance;

public class Utils
{
    public static boolean BYPASS_COMPRESSION = true;

    public static boolean isGenericClass(Class clazz)
    {
        return clazz.getGenericSuperclass() instanceof ParameterizedType;
    }

    public static Class<?> getClassFromType(Type ty)
    {
        return TypeToken.of(ty).getRawType();
    }

    public static Class<?> getClassGeneric(Object o)
    {
        Class<?> clazz;
        if (isGenericClass(o.getClass()))
        {
            clazz = getClassFromType(o.getClass().getGenericSuperclass());
        }
        else
        {
            clazz = o.getClass();
        }
        return clazz;
    }

    public static Map<String, String> convertObjectFieldToMap(Object o) throws Exception
    {
        Class<?> clazz = getClassGeneric(o);
        Map<String, String> fields = new HashMap<>();
        for (Field f : ReflectionUtils.getAllFields(clazz))
        {
            f.setAccessible(true);
            if (java.util.Objects.isNull(f.get(o)))
            {
                fields.put(f.getName(), "##NULL##");
            }
            else
            {
                fields.put(f.getName(), f.get(o).toString());
            }
        }
        return fields;
    }

    public static Object getObjectField(Object o, String targetField) throws Exception
    {
        if (java.util.Objects.isNull(o)) return null;
        Class<?> clazz = getClassGeneric(o);
        for (Field f : ReflectionUtils.getAllFields(clazz))
        {
            f.setAccessible(true);
            String fieldName = f.getName();
            if (fieldName.equals(targetField))
            {
                return f.get(o);
            }
        }
        return null;
    }

    public static boolean isJavaLang(Object o)
    {
        return o.getClass().getName().startsWith("java.lang");
    }

    public static Object invokeMethodNoArg(Object target, String methodName) throws Exception
    {
        Method method = target.getClass().getMethod(methodName);
        Object r = method.invoke(target);
        return r;
    }

    public static boolean hasMethod(Object target, String methodName)
    {
        try
        {
            target.getClass().getMethod(methodName);
        }
        catch (NoSuchMethodException e)
        {
            return false;
        }
        return true;
    }

    public static String getCurrentJvmMainClass()
    {
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet())
        {
            Thread thread = entry.getKey();
            if (thread.getThreadGroup() != null && thread.getThreadGroup().getName().equals("main"))
            {
                for (StackTraceElement stackTraceElement : entry.getValue())
                {
                    if (stackTraceElement.getMethodName().equals("main"))
                    {

                        try
                        {
                            Class<?> c = Class.forName(stackTraceElement.getClassName());
                            Class[] argTypes = new Class[] { String[].class };
                            // This will throw NoSuchMethodException in case of fake main methods
                            c.getDeclaredMethod("main", argTypes);
                            return stackTraceElement.getClassName();
                        }
                        catch (NoSuchMethodException e)
                        {
                            return "";
                        }
                        catch (ClassNotFoundException e)
                        {
                            return "";
                        }
                    }
                }
            }
        }
        return "";
    }

    // Doesn't work
    public static byte[] zstdStringCompress(String input) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZstdCompressorOutputStream zcos = new ZstdCompressorOutputStream(baos);
        PrintWriter out = new PrintWriter(zcos);
        out.write(input);
        out.close();
        zcos.close();
        baos.flush();
        return baos.toByteArray();
    }

    // Doesn't work
    public static String zstdStringDecompress(byte[] input) throws IOException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        ZstdCompressorInputStream zcis = new ZstdCompressorInputStream(bais);
        byte[] tmp = ByteStreams.toByteArray(zcis);
        return new String(tmp);
    }

    public static byte[] lz4StringCompress(String input) throws IOException
    {
        if (BYPASS_COMPRESSION) return input.getBytes();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FramedLZ4CompressorOutputStream flcos = new FramedLZ4CompressorOutputStream(baos);
        flcos.write(input.getBytes());
        flcos.close();
        baos.flush();
        return baos.toByteArray();
    }

    public static String lz4StringDecompress(byte[] input) throws IOException
    {
        if (BYPASS_COMPRESSION) return new String(input);
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        FramedLZ4CompressorInputStream flcis = new FramedLZ4CompressorInputStream(bais, true);
        byte[] tmp = ByteStreams.toByteArray(flcis);
        return new String(tmp);
    }

    public static List<String> maskedHBaseRPC = Arrays.asList("Multi");

    public static boolean isMaskedHBaseRPC(String methodName)
    {
        return maskedHBaseRPC.stream().anyMatch(e -> methodName.contains(e));
    }

    public static Instant protoTimestampToInstant(Timestamp ts)
    {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    public static Timestamp instantToProtoTimestamp(Instant instant)
    {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    public static java.time.Duration protoDurationToJavaDuration(pfl.shaded.com.google.protobuf.Duration duration)
    {
        return java.time.Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    }

    public static double stringSimilarity(String s1, String s2)
    {
        String longer = s1, shorter = s2;
        if (s1.length() < s2.length())
        {
            longer = s2;
            shorter = s1;
        }
        int longerLength = longer.length();
        if (longerLength == 0) return 1.0;
        return (longerLength - getLevenshteinDistance(longer, shorter)) / (double) longerLength;
    }

    private static int getLevenshteinDistance(String s1, String s2)
    {
        LevenshteinDistance dist = LevenshteinDistance.getDefaultInstance();
        return dist.apply(s1, s2);
    }
}
