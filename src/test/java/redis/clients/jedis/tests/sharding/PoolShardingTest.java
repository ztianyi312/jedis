package redis.clients.jedis.tests.sharding;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.sharding.CacheAction;
import redis.clients.jedis.sharding.ExtendedJedisShardInfo;
import redis.clients.jedis.sharding.JedisAgent;
import redis.clients.jedis.sharding.PoolSharding;
import redis.clients.jedis.sharding.ShardingPipeline;


public class PoolShardingTest {

    private PoolSharding ShardingPool;
    
    @Before
    public void setUp() throws Exception {
        ShardingPool = new PoolSharding(new JedisPoolConfig(), "114.113.201.39:22081 114.113.201.39:22082 114.113.201.39:22083", 2);
    } 
    
    @Test
    public void checkConnections() {
        JedisPool jedisPool = ShardingPool.getShard("foo");
        Jedis jedis = jedisPool.getResource();
        jedis.set("foo", "bar");
        assertEquals("bar", jedis.get("foo"));
        jedisPool.returnResource(jedis);
    }
    
    @Test
    public void rebuild() throws InterruptedException {
        
        for(int i=1; i<4; i++) {
            JedisPool jedisPool = ShardingPool.getShard("_"+i);
            Jedis jedis = jedisPool.getResource();
            jedis.set("_"+i, String.valueOf(i));
            jedisPool.returnResource(jedis);
        }
        
        ExtendedJedisShardInfo shardInfo = ShardingPool.getShardInfo("_1");
        shardInfo.getErrCount().incrementAndGet();
        shardInfo.getErrCount().incrementAndGet();
        ShardingPool.rebuildNodes();
        
        JedisPool jedisPool = ShardingPool.getShard("_i");
        Jedis jedis = jedisPool.getResource();
        assertNull(jedis.get("_1"));
        jedisPool.returnResource(jedis);
        
        Thread.sleep(20000);
        
        jedisPool = ShardingPool.getShard("_i");
        jedis = jedisPool.getResource();
        assertEquals("1", jedis.get("_1"));
        jedisPool.returnResource(jedis);
    }
    
    

}
