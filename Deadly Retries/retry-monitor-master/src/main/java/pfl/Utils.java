package pfl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import pfl.shaded.com.google.common.io.ByteStreams;
import pfl.shaded.com.google.protobuf.Message;
import pfl.shaded.com.google.protobuf.Parser;
import pfl.shaded.com.google.protobuf.Timestamp;
import pfl.shaded.org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import pfl.shaded.org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import pfl.shaded.org.apache.commons.text.similarity.LevenshteinDistance;

public class Utils
{
    public static boolean BYPASS_COMPRESSION = true;

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

    public static String lz4StringDecompress(byte[] input) throws IOException
    {
        if (BYPASS_COMPRESSION) return new String(input);
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        FramedLZ4CompressorInputStream flcis = new FramedLZ4CompressorInputStream(bais, true);
        byte[] tmp = ByteStreams.toByteArray(flcis);
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

    public static void serializeProtobuf(Message msg, Path path) throws IOException
    {
        try (OutputStream os = Files.newOutputStream(path);
             FramedLZ4CompressorOutputStream flcos = new FramedLZ4CompressorOutputStream(os);)
        {
            msg.writeTo(flcos);
        }
        catch (IOException e)
        {
            throw e;
        }
    }

    public static <T extends Message> T deserializeProtobuf(Path path, Parser<T> parser) throws IOException
    {
        try (InputStream is = Files.newInputStream(path);
             FramedLZ4CompressorInputStream flcis = new FramedLZ4CompressorInputStream(is, true);)
        {
            T t = parser.parseFrom(flcis);
            return t;
        }
        catch (IOException e)
        {
            throw e;
        }
    }

    public static Instant protoBufTStoInstant(Timestamp ts)
    {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    public static Timestamp instantToProtoTimestamp(Instant instant)
    {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    public static pfl.shaded.com.google.protobuf.Duration javaDurationToProtoDuration(java.time.Duration duration)
    {
        return pfl.shaded.com.google.protobuf.Duration.newBuilder().setSeconds(duration.getSeconds()).setNanos(duration.getNano()).build();
    }
}
