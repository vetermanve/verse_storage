<?php


namespace Verse\Storage\Data;


use Mu\Cache\Redis;
use Mu\Env;
use Verse\Storage\Request\StorageDataRequest;

class RedisCacheDataAdapter extends DataAdapterProto
{
    const DEFAULT_TTL = 600; // 10min
    
    /**
     * @var Redis
     */
    private $redis;
    
    private $prefix = 'storageCache';
    
    private $ttl = self::DEFAULT_TTL;
    
    /**
     * RedisCacheDataAdapter constructor.
     *
     * @param string $prefix
     * @param int    $ttl
     */
    public function __construct($prefix, $ttl = self::DEFAULT_TTL)
    {
        $this->prefix = $prefix;
        $this->ttl    = $ttl;
    }
    
    private function getRedis () 
    {
        !$this->redis && $this->redis = Env::getRedis();
        
        return $this->redis;
    }
    
    /**
     * @param $insertBind
     *
     * @return StorageDataRequest
     */
    public function getInsertRequest($id, $insertBind)
    {
        return new StorageDataRequest(
            [
                $this->getRedis(), 
                $this->ttl, 
                $this->getKey($id), 
                $insertBind
            ],
            function (Redis $redis, $ttl, $id, $insertBind) {
                return $redis->set($id, \json_encode($insertBind), $ttl);
            }
        );
    }
    
    /**
     * @param $insertBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchInsertRequest($insertBindsByKeys)
    {
        $writeData = [];
        foreach ($insertBindsByKeys as $key => $bind) {
            $writeData[$this->getKey($key)] = \json_encode($bind);
        }
        
        return new StorageDataRequest(
            [
                $this->getRedis(),
                $this->ttl,
                $writeData,
            ],
            function (Redis $redis, $ttl, $writeData) {
                return $redis->mset($writeData, $ttl);
            }
        );
    }
    
    /**
     * @param $id
     * @param $updateBind
     *
     * @return StorageDataRequest
     */
    public function getUpdateRequest($id, $updateBind)
    {
        return $this->getInsertRequest($id, $updateBind);
    }
    
    /**
     * @param $updateBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchUpdateRequest($updateBindsByKeys)
    {
        return $this->getBatchInsertRequest($updateBindsByKeys);
    }
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest
     */
    public function getDeleteRequest($ids)
    {
        return new StorageDataRequest(
            [
                $this->getRedis(), 
                $this->getKey($ids)
            ],
            function (Redis $redis, $id) {
                return $redis->remove($id);
            }
        );
    }
    
    /**
     * @param       $filter
     *
     * @param int   $limit
     * @param array $conditions
     *
     * @return StorageDataRequest
     */
    public function getSearchRequest($filter, $limit = 1, $conditions = [])
    {
        if (isset($filter['id'])) {
            $ids = is_array($filter['id']) ? $filter['id'] : [$filter['id']];
            return $this->getReadRequest($ids);
        }
        
        return new StorageDataRequest();
    }
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest
     */
    public function getReadRequest($ids)
    {
        $ids = array_unique(array_filter($ids));
        return new StorageDataRequest(
            [
                $this->getRedis(),
                $this->getKeys($ids, false),
                 $this->prefix
            ],
            function (Redis $redis, $ids, $prefix) {
                $resultPacked = $redis->mget($ids);
                $results = [];
                $prefixLen = strlen($prefix) + 1;
                
                foreach (array_filter($resultPacked) as $key => $packedData) {
                    $data = \json_decode($packedData, 1);
                    
                    if ($data) {
                        $results[substr($key, $prefixLen)] = $data;
                    }
                }
                
                return $results;
            }
        );
    }
    
    private function getKey ($id) 
    {
        return $this->prefix.':'.$id;
    }
    
    private function getKeys ($ids, $presentKeys = true) 
    {
        $keys = [];
        foreach ($ids as $id) {
            $keys[$id] = $this->prefix.':'.$id;
        }
        return $presentKeys ? $keys : array_values($keys);
    }
}