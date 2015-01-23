package redis.clients.jedis.sharding;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;
import redis.clients.util.SafeEncoder;

public class PoolSharding {
    
    public static final int REDIS_TIMEOUT = 1000;
    
    public PoolSharding(JedisPoolConfig config, String address, Hashing algo, int maxFailNum) {
        infoList = buildShardInfos(address, null);
        resources = Collections.unmodifiableMap(buildShardInfos(config, infoList));
        this.algo = algo;
        this.maxFailNum = maxFailNum;
        initialize(infoList);
    }
    
    public PoolSharding(JedisPoolConfig config, String address, int maxFailNum) {
        this(config, address,  Hashing.MURMUR_HASH, maxFailNum);
        
    }
    
    public PoolSharding(JedisPoolConfig config, String address, Pattern tagPattern, int maxFailNum) {
        this(config, address,  Hashing.MURMUR_HASH, maxFailNum);
        this.tagPattern = tagPattern;
    }
    
    public static final int DEFAULT_WEIGHT = 1;
    private volatile TreeMap<Long, ExtendedJedisShardInfo> nodes;
    private final Hashing algo;
    private Map<ExtendedJedisShardInfo, JedisPool> resources ;
    private List<ExtendedJedisShardInfo> infoList;

    private int maxFailNum = 10;
    
    private ScheduledExecutorService refreshService = Executors.newSingleThreadScheduledExecutor();
    /**
     * The default pattern used for extracting a key tag. The pattern must have
     * a group (between parenthesis), which delimits the tag to be hashed. A
     * null pattern avoids applying the regular expression for each lookup,
     * improving performance a little bit is key tags aren't being used.
     */
    private Pattern tagPattern = null;
    // the tag is anything between {}
    public static final Pattern DEFAULT_KEY_TAG_PATTERN = Pattern
            .compile("\\{(.+?)\\}");

    private void initialize(List<ExtendedJedisShardInfo> shards) {
        nodes = new TreeMap<Long, ExtendedJedisShardInfo>();
        
        for (int i = 0; i != shards.size(); ++i) {
            final ExtendedJedisShardInfo shardInfo = shards.get(i);
            if (shardInfo.getName() == null) {
                for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
                    nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), shardInfo);
                }
            } else {
                for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
                    nodes.put(this.algo.hash(shardInfo.getName() + "*" + shardInfo.getWeight() + n), shardInfo);
                }
            }
        }
    }


    public synchronized void rebuildNodes() {
        TreeMap<Long, ExtendedJedisShardInfo> nodes = new TreeMap<Long, ExtendedJedisShardInfo>();
        for (int i = 0; i != infoList.size(); ++i) {
            final ExtendedJedisShardInfo shardInfo = infoList.get(i);
            
            if(shardInfo.getStatus() == 0) {
                if(shardInfo.getErrCount().get() < maxFailNum) {
                    if (shardInfo.getName() == null) {
                        for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
                            nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), shardInfo);
                        }
                    } else {
                        for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
                            nodes.put(this.algo.hash(shardInfo.getName() + "*" + shardInfo.getWeight() + n), shardInfo);
                        }
                    }
                } else {
                    shardInfo.setStatus(1);
                    retry(shardInfo);
                }
            
            }
        }
        
        this.nodes = nodes;
    }
    
    protected synchronized void retry(final ExtendedJedisShardInfo shardInfo) {
        refreshService.schedule(new Runnable() {
            @Override
            public void run() {
                JedisPool jedisPool = resources.get(shardInfo);
                Jedis jedis = null;
                try {
                    jedis = jedisPool.getResource();
                    if(jedis.isConnected() && jedis.ping().equals("PONG")){
                        shardInfo.getErrCount().set(0);
                        shardInfo.setStatus(0);
                        rebuildNodes();
                    }
                } catch (Exception e) {
                    retry(shardInfo);
                } finally {
                    if(jedis != null) {
                        jedisPool.returnResource(jedis);
                    }
                }
            }
        }, 10l, TimeUnit.SECONDS);
    }
    
    public ExtendedJedisShardInfo getShardInfo(byte[] key) {
        SortedMap<Long, ExtendedJedisShardInfo> tail = nodes.tailMap(algo.hash(key));
        if (tail.size() == 0) {
            return nodes.get(nodes.firstKey());
        }
        return tail.get(tail.firstKey());
    }

    public ExtendedJedisShardInfo getShardInfo(String key) {
        return getShardInfo(SafeEncoder.encode(getKeyTag(key)));
    }
    
    public JedisPool getShard(byte[] key) {
        return resources.get(getShardInfo(key));
    }

    public JedisPool getShard(String key) {
        return getShard(SafeEncoder.encode(getKeyTag(key)));
    }
    
    public JedisPool getShard(ExtendedJedisShardInfo shardInfo) {
        return resources.get(shardInfo);
    }

    /**
     * A key tag is a special pattern inside a key that, if preset, is the only
     * part of the key hashed in order to select the server for this key.
     *
     * @see http://code.google.com/p/redis/wiki/FAQ#I
     *      'm_using_some_form_of_key_hashing_for_partitioning,_but_wh
     * @param key
     * @return The tag if it exists, or the original key
     */
    public String getKeyTag(String key) {
        if (tagPattern != null) {
            Matcher m = tagPattern.matcher(key);
            if (m.find())
                return m.group(1);
        }
        return key;
    }
    
    private static List<ExtendedJedisShardInfo> buildShardInfos(String address, String password) {
        List<ExtendedJedisShardInfo> shards = new ArrayList<ExtendedJedisShardInfo>();
        for(String addr : address.split(" ")) {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            ExtendedJedisShardInfo info = new ExtendedJedisShardInfo(host, port, REDIS_TIMEOUT);
            if (password != null) {
                info.setPassword(password);
            }
            shards.add(info);
        }
        return shards;
    }
    
    private static Map<ExtendedJedisShardInfo, JedisPool> buildShardInfos(JedisPoolConfig config, List<ExtendedJedisShardInfo> infoList) {
        Map<ExtendedJedisShardInfo, JedisPool> map = new LinkedHashMap<ExtendedJedisShardInfo, JedisPool>();
        
        for(ExtendedJedisShardInfo info : infoList) {
            JedisPool pool = new JedisPool(config, info.getHost(), info.getPort(), info.getTimeout(), info.getPassword());
            map.put(info, pool);
        }
        
        return map;
    }
    
    public List<GenericObjectPool> getCorePool(){
        try{
            Field poolField = Pool.class.getDeclaredField("internalPool");
            poolField.setAccessible(true);
            List<GenericObjectPool> list = new ArrayList<GenericObjectPool>();
            
            for(Map.Entry<ExtendedJedisShardInfo, JedisPool> entry:resources.entrySet()) {
                GenericObjectPool gpool = (GenericObjectPool)poolField.get(entry.getValue());
                //LOGGER.info("ExtendedJedisShardInfo:{} NumActive:{} NumIdle:{}", new Object[] {entry.getKey(), gpool.getNumActive(), gpool.getNumIdle()});
                list.add(gpool);
            }     
            
            return list;
        }catch(Exception e) {
            //LOGGER.error("Field",e);
            return null;
        }

    }

    public int getMaxFailNum() {
        return maxFailNum;
    }
    
    
}
