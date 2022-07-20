<?php


namespace Verse\Storage\Data;


use Verse\Storage\Connector\Redis;
use Verse\Di\Env;
use Verse\Storage\Request\StorageDataRequest;
use Verse\Storage\Util\Uuid;

class RedisCacheDataAdapter extends DataAdapterProto
{
    const DEFAULT_TTL = 600; // 10min
    
    /**
     * @var Redis
     */
    private $redis;
    
    private $prefix = 'storageCache';
    
    private $ttl = self::DEFAULT_TTL;

    private array $primaryKeyParts = ['id'];

    public function setPrimaryKey($primaryKey) : void
    {
        $this->primaryKeyParts = [];
        foreach (explode(',', $primaryKey) as $item) {
            $this->primaryKey[] = trim($item);
        }

        parent::setPrimaryKey($primaryKey);
    }

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
        !$this->redis && $this->redis = Env::getContainer()->bootstrap(Redis::class);
        
        return $this->redis;
    }

    /**
     * @param $id string|array
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
                $this->prefix,
                $this->primaryKeyParts,
                $this->getKey($id), 
                $insertBind
            ],
            function (Redis $redis, $ttl, $prefix, $primaryKeyParts, $key, $insertBind) {
                $result = $redis->setnx($prefix.':'. $key, \json_encode($insertBind), $ttl);

                if ($result) {
                    if (count($primaryKeyParts) > 1) {
                        $keyParts = explode(':', $key);
                        if (count($primaryKeyParts) === count($keyParts)) {
                            return array_combine($primaryKeyParts, $keyParts) + $insertBind;
                        } else {
                            throw new \Exception("Count of primary key parts not equals count of key parts");
                        }
                    } else {
                        return [$primaryKeyParts[0] => $key] + $insertBind;
                    }
                }

                return $result;
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
            $writeData[$this->getKey($key)] = $bind;
        }
        
        return new StorageDataRequest(
            [
                $this->getRedis(),
                $this->ttl,
                $this->prefix,
                $writeData,
            ],
            function (Redis $redis, $ttl, $prefix, $writeData) {
                $result = [];

                foreach ($writeData as $key => $record) {
                    $res = $redis->setnx($prefix.':'.$key, \json_encode($record), $ttl);
                    $result[$key] = $res ? [$this->primaryKey => $key] + $record : false;
                }

                return $result;
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
        return new StorageDataRequest(
            [
                $this->getRedis(),
                $this->ttl,
                $this->prefix,
                $this->primaryKeyParts,
                $this->getKey($id),
                $updateBind
            ],
            function (Redis $redis, $ttl, $prefix, $primaryKeyParts, $key, $insertBind) {
                $result = $redis->set($prefix.':'. $key, \json_encode($insertBind), $ttl);

                if ($result) {
                    if (count($primaryKeyParts) > 1) {
                        $keyParts = explode(':', $key);
                        if (count($primaryKeyParts) === count($keyParts)) {
                            return array_combine($primaryKeyParts, $keyParts) + $insertBind;
                        } else {
                            throw new \Exception("Count of primary key parts not equals count of key parts");
                        }
                    } else {
                        return [$primaryKeyParts[0] => $key] + $insertBind;
                    }
                }

                return $result;
            }
        );
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
//        $ids = array_unique(array_filter($ids));
        return new StorageDataRequest(
            [
                $this->getRedis(),
                $this->getKeys($ids, false),
                $this->prefix,
                $this->primaryKeyParts
            ],
            function (Redis $redis, $ids, $prefix, $primaryKeyParts) {
                $resultPacked = $redis->mget($ids);
                $results = [];
                $prefixLen = strlen($prefix) + 1;
                
                foreach (array_filter($resultPacked) as $keyWithPrefix => $packedData) {
                    $data = \json_decode($packedData, 1);

                    if ($data) {
                        $key = substr($keyWithPrefix, $prefixLen);
                        if (count($primaryKeyParts) > 1) {
                            $keyParts = explode(':', $key);
                            if (count($primaryKeyParts) === count($keyParts)) {
                                $results[$key] = array_combine($primaryKeyParts, $keyParts) + $data;
                            } else {
                                throw new \Exception("Count of primary key parts not equals count of key parts");
                            }
                        } else {
                            $results[$key] = [$primaryKeyParts[0] => $key] + $data;
                        }
                    }
                }
                
                return $results;
            }
        );
    }
    
    private function getKey ($id) : string
    {
        if (!is_array($id)) {
            return $id ? $id : Uuid::v4();
        }

        foreach ($id as &$keyPart) {
            $keyPart = $keyPart ?? Uuid::v4();
        }

        return implode(':', $id);
    }
    
    private function getKeys ($ids, $presentKeys = true) 
    {
        $keys = [];
        foreach ($ids as $id) {
            $key = $this->getKey($id);
            $keys[$key] = $this->prefix.':'.$key;
        }
        return $presentKeys ? $keys : array_values($keys);
    }
}