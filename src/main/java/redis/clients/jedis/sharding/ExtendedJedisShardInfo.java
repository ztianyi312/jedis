package redis.clients.jedis.sharding;

import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.JedisShardInfo;

public class ExtendedJedisShardInfo extends JedisShardInfo {
    
    private AtomicInteger errCount = new AtomicInteger();
    
    private int status;

    public ExtendedJedisShardInfo(String host, int port, int timeout) {
        super(host, port, timeout);
    }

    public AtomicInteger getErrCount() {
        return errCount;
    }

    public void setErrCount(AtomicInteger errCount) {
        this.errCount = errCount;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    
}
