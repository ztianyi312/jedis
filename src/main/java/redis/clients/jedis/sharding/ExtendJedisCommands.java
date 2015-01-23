package redis.clients.jedis.sharding;


public interface ExtendJedisCommands {

    public void printPoolStatus();
    public void setObject(String key, Object o);
    public Object getObject(String key, Class clazz);
}
