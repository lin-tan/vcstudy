package pfl;

public class Config
{   
    public static long FATAL_THRESHOLD = 30000; // HBase-14598: 10000ms; HBase-23076: 30000ms
    public static double SYSLOG_SIMILARITY_THRESHOLD = 0.9;
    public static double RPC_PARAM_SIMILARITY_THRESHOLD = 0.9;

    public static String RT_SERIALIZER_ROOTPATH = "/tmp/retry-monitor-serialization";
}
