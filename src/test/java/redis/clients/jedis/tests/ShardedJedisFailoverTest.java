package redis.clients.jedis.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class ShardedJedisFailoverTest extends Assert {
    
    private List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
    
    private List<ShardedJedis> jedisList = new ArrayList<ShardedJedis>();
    
    private ExecutorService executor = Executors.newFixedThreadPool(3);

    @Before
    public void startUp() {
        shards.add(new JedisShardInfo("love39", 22081));
        shards.add(new JedisShardInfo("love39",22082));
        shards.add(new JedisShardInfo("love39",22084));
        shards.add(new JedisShardInfo("love39",22085));
    }
    
    //@Test
    public void trySharding() {
        
        


        ShardedJedis jedisShard1 = new ShardedJedis(shards);
        ShardedJedis jedisShard2 = new ShardedJedis(shards);
        ShardedJedis jedisShard3 = new ShardedJedis(shards);
        
        
        System.out.println(Jedis.executor);
        System.out.println(BinaryJedis.executor);

        for(int i=0; i<300; i++) {
            try {
                jedisShard1.get("key_"+i);
                jedisShard2.get("key_"+(i+1));
                jedisShard3.get("key_"+(i+2));
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (Jedis jedis : jedisShard1.getAllShards()) {
            //assertTrue(jedis.isConnected());
            System.out.println(jedis.isConnected());
        }
    }
    @Test
    public void addConnection(){
        List<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
        list.add(new JedisShardInfo("love39",22085));
        
        for(int i=0; i<3000; i++) {
            try {
                ShardedJedis jedisShard = new ShardedJedis(list);
                jedisShard.get("ad");
                jedisList.add(jedisShard);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    @Test
    public void tryPoolSharding() throws InterruptedException {
        
        System.out.println(jedisList.size());
        
        final ShardedJedisPool pool = new ShardedJedisPool(
                new GenericObjectPoolConfig(), shards);
        
        for(int i=0; i<300000; i++) {
            
            final int index =i;
            executor.execute(new Runnable(){

                @Override
                public void run() {
                    ShardedJedis jedis = pool.getResource();
                    boolean isFailed = false;
                    try {
                        jedis.get("key_"+index);
                    } catch (Exception e) {
                        e.printStackTrace();
                        isFailed = true;
                    }finally{
                        if (isFailed) {
                            pool.returnBrokenResource(jedis);
                        } else {
                            pool.returnResource(jedis);
                        }
                    }
                    
                    
                }
                
            });
        }
       
        executor.shutdown();
       
        executor.awaitTermination(30, TimeUnit.MINUTES);
        pool.destroy();
    }
}
