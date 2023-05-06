package pfl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class InstConfig 
{
    public int RECV_ARG_OFFSET;

    public InstConfig() throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("inst.config");
        Properties prop = new Properties();
        prop.load(is);
        RECV_ARG_OFFSET = Integer.parseInt(prop.getProperty("rpc.receive.argOffset"));
    }
}
