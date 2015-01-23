package redis.clients.jedis.sharding;

import static java.lang.reflect.Proxy.newProxyInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class JedisAgent{

    private PoolSharding poolSharding;
    
    public JedisAgent(PoolSharding poolSharding) {
        this.poolSharding = poolSharding;
    }
    
    
    public List<Object> operate(CacheAction action) {
        ShardingPipeline pipline = new ShardingPipeline();
        pipline.setShardedJedis(poolSharding);
        
        try{
            action.process(pipline);
            return pipline.syncAndReturnAll();
        }catch(JedisException e) {
            ExtendedJedisShardInfo shardInfo = pipline.getCurrentShardInfo();
            if(shardInfo != null && shardInfo.getErrCount().incrementAndGet() >= poolSharding.getMaxFailNum()) {
                poolSharding.rebuildNodes();
            }
            throw e;
        }finally {
            pipline.releaseResource();
        }
    }
    
    public void SyncOperate(CacheAction action) {
        ShardingPipeline pipline = new ShardingPipeline();
        pipline.setShardedJedis(poolSharding);
        try{
            action.process(pipline);
            pipline.sync();
        }catch(JedisException e) {
            ExtendedJedisShardInfo shardInfo = pipline.getCurrentShardInfo();
            if(shardInfo != null && shardInfo.getErrCount().incrementAndGet() >= poolSharding.getMaxFailNum()) {
                poolSharding.rebuildNodes();
            }
            throw e;
        }
        finally {
            pipline.releaseResource();
        }
    }
    
    public List<GenericObjectPool> getCorePool() {
        return this.poolSharding.getCorePool();
    }
 
    public JedisClient getJedisProxy(){
        return (JedisClient)newProxyInstance(
                JedisAgent.class.getClassLoader(), 
                new Class[] {JedisClient.class}, 
                new JedisInterceptor());
    }
    
    private class JedisInterceptor implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            
            if(method.getDeclaringClass().isAssignableFrom(JedisAgent.this.getClass())){

                return method.invoke(JedisAgent.this, args);

            } else if (args.length > 0){
                
                ExtendedJedisShardInfo shardInfo = null;
                if(args[0] instanceof String) {
                    shardInfo = poolSharding.getShardInfo((String)args[0]);
                } else {
                    shardInfo = poolSharding.getShardInfo((byte[])args[0]);
                }
                
                JedisPool pool = poolSharding.getShard(shardInfo);
                Jedis jedis = null;
                try {
                    jedis = pool.getResource();
                    Object result = method.invoke(jedis, args);
                    if(shardInfo.getErrCount().get() != 0) {
                        shardInfo.getErrCount().set(0);
                    }
                    
                    return result;
                }catch(JedisException e) {
                    
                    if(shardInfo.getErrCount().incrementAndGet() >= poolSharding.getMaxFailNum()) {
                        poolSharding.rebuildNodes();
                    }
                    throw e;
                    //return null;
                } finally {
                    if(jedis != null) pool.returnResource(jedis);
                }
                
            } else {
                throw new JedisException("args err!");
            }
            
           
        }
        
    }
    
}
