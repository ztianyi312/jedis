package redis.clients.jedis.tests.sharding;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.sharding.CacheAction;
import redis.clients.jedis.sharding.JedisAgent;
import redis.clients.jedis.sharding.JedisClient;
import redis.clients.jedis.sharding.PoolSharding;
import redis.clients.jedis.sharding.ShardingPipeline;

public class JedisAgentTest {

    private PoolSharding ShardingPool;
    
    private JedisAgent jedisAgent;
    
    private JedisClient jedisClient;
    
    @Before
    public void setUp() throws Exception {
        ShardingPool = new PoolSharding(new JedisPoolConfig(), "114.113.201.39:22081 114.113.201.39:22082 114.113.201.39:22087", 2);
        jedisAgent = new JedisAgent(ShardingPool);
        
        jedisClient = jedisAgent.getJedisProxy();
    } 
    
    
    //@Test
    public void connectionManage() {
        jedisClient.set("_4", "4");
        assertEquals("4", jedisClient.get("_4"));
        jedisClient.del("_4");
        assertNull(jedisClient.get("_4"));

        List<GenericObjectPool> list = jedisAgent.getCorePool();
        
        for(GenericObjectPool pool : list) {
            assertEquals(pool.getNumActive(), 0);
        }
    }
    
    @Test
    public void poolTest() throws IOException {
        JedisAgent agent = new JedisAgent(ShardingPool);
        
        try{
            agent.operate(new CacheAction() {
            
                @Override
                public void process(ShardingPipeline pipline) {
                    pipline.set("_1", "1");
                    pipline.set("_2", "2");
                    pipline.set("_3", "3");
                    pipline.set("_4", "4");
                }
            });   
        }catch(Exception e) {
            System.out.println(e.getMessage());
        }

        /*
         
        
        
        for(int i=0; i<50; i++) {
            
            System.out.println(jedisClient.get("_2"));
            System.out.println(jedisClient.get("_3"));
            System.out.println(jedisClient.get("_4"));
            System.out.println(jedisClient.get("_1"));
        }*/
        
        for(int i=0; i<20; i++) {
            try{
                List<Object> list = agent.operate(new CacheAction() {
            
                    @Override
                    public void process(ShardingPipeline pipline) {
                        pipline.get("_1");
                        pipline.get("_2");
                        pipline.get("_3");
                        pipline.get("_4");
                    }
                });    
                for(Object o : list) {
                    System.out.println(o);
                }
            }catch(Exception e) {
                System.out.println(e.getMessage());
            }
            
            List<GenericObjectPool> poolList = jedisAgent.getCorePool();
            
            for(GenericObjectPool pool : poolList) {
                assertEquals(pool.getNumActive(), 0);
            }
        }
    }
}
