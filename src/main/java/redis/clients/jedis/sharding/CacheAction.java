package redis.clients.jedis.sharding;


public interface CacheAction {

    public void process(ShardingPipeline pipline) ;
}
