<?php

namespace Verse\Storage\Connector;

use Exception;
use \RedisException;

/**
 * Class Redis
 */
class Redis
{

    const MAX_RECONNECT_TRY = 5;
    const RECONNECTS_TIMEOUT = 1;
    
    const DEFAULT_TTL = 3600;

    /**
     * @var string
     */
    protected $_host = 'localhost';

    /**
     * @var int
     */
    protected $_port = 6379;

    /**
     * @var string
     */
    protected $_password = false;

    /**
     * @var float
     */
    protected $_connectionTimeout = 0.00;

    /**
     * @var float
     */
    protected $_readTimeout = 3;

    /**
     * @var \Redis
     */
    private $_instance = null;

    /**
     * @var integer
     */
    private $reconnectIterator = 1;

    /**
     * @inheritdoc
     */
    public function __construct($params)
    {
        if (isset($params['host'])) {
            $this->_host = $params['host'];
        }

        if (isset($params['port'])) {
            $this->_port = $params['port'];
        }

        if (isset($params['password'])) {
            $this->_password = $params['password'];
        }

        if (isset($params['connection_timeout'])) {
            $this->_connectionTimeout = (float)$params['connection_timeout'];
        }

        if (isset($params['read_timeout'])) {
            $this->_readTimeout = (float)$params['read_timeout'];
        }
        
        $this->_init();
    }
    
    
    /**
     * @inheritdoc
     */
    protected function _init()
    {
        try {
            $this->_instance = new \Redis();
            $this->_instance->pconnect($this->_host, $this->_port, $this->_connectionTimeout);
            
            if ($this->_password) {
                $this->_instance->auth($this->_password);
            }
            
            $this->_instance->setOption(\Redis::OPT_READ_TIMEOUT, $this->_readTimeout);
        } catch (Exception $e) {
            throw new \RuntimeException('Redis connect exception: ' . $e->getMessage());
        }
    }

    /**
     * @inheritdoc
     */
    public function get($key)
    {
        return $this->_call(__FUNCTION__, [$key]);
    }

    /**
     * @inheritdoc
     */
    public function incr($key)
    {
        return $this->_call(__FUNCTION__, [$key]);
    }

    /**
     * @inheritdoc
     */
    public function incrBy($key, $value)
    {
        return $this->_call(__FUNCTION__, [$key, $value]);
    }

    /**
     * @inheritdoc
     */
    public function put($key, $value, array $params = [])
    {
        $exp = isset($params['expiration']) ? $params['expiration'] : 0;
        $tags = isset($params['tags']) ? $params['tags'] : [];

        if ($tags) {
            $this->updateTagsReferences($key, (array)$params['tags']);
        }

        return $this->_call('set', [$key, $value, $exp]);
    }
    
    public function set ($key, $value, $exp)
    {
        return $this->_call('set', [$key, $value, $exp]);
    }

    public function setnx($key, $value, $ttl = 0)
    {
        $ttl = (int)$ttl;

        if ($ttl > 0) {
            /**
             * супер-хак для setnx с ttl
             * @see https://github.com/phpredis/phpredis#set
             */
            return $this->_call('set', [$key, $value, ['nx', 'ex' => $ttl]]);
        } else {
            return $this->_call(__FUNCTION__, [$key, $value]);
        }
    }

    public function expire($key, $ttl)
    {
        return $this->_call('expire', [$key, $ttl]);
    }
    
    public function zincrby($scope, $key, $value)
    {
        return $this->_call('zincrby', [$scope, $value, $key]);
    }

    /**
     * @inheritdoc
     */
    public function remove($key)
    {
        return $this->_call('del', [$key]) > 0;
    }
    
    /**
     * @return \Redis
     */
    public function getInstance()
    {
        return $this->_instance;
    }

    /**
     * @param string $key
     * @return integer
     * @throws Exception|RedisException
     */
    public function ttl($key)
    {
        return $this->_call(__FUNCTION__, [$key]);
    }
    
    public function hset ($scope, $key, $val, $scopeTtl = null) 
    {
        $this->_call('hSet', [$scope, $key, $val]);
        
        if ($scopeTtl !== null) {
            $this->_call('expire', [$scope, $scopeTtl]);
        }
        
        return $val;
    }

    public function sadd ($key, $val)
    {
        return $this->_call('sAdd', [$key, $val]);
    }


    public function srem ($key, $val)
    {
        return $this->_call('sRem', [$key, $val]);
    }

    public function spop ($key)
    {
        return $this->_call('sPop', [$key]);
    }

    /**
     * @param string $scope
     * @return bool|int|string|array
     * @throws Exception|RedisException
     */
    public function hgetall ($scope)
    {
        return $this->_call(__FUNCTION__, [$scope]);
    }

    /**
     * @param string $scope
     * @param array $keys
     * @return bool|int|string
     * @throws Exception|RedisException
     */
    public function hmget ($scope, $keys)
    {
        return $this->_call('hMGet', [$scope, $keys]);
    }
    
    /**
     * @param string $scope
     * @param array  $dataByKeys
     * 
     * @return bool|int|string
     * @throws Exception|RedisException
     */
    public function hmset ($scope, $dataByKeys)
    {
        return $this->_call('hMSet', [$scope, $dataByKeys]);
    }
    
    /**
     * @param array $dataByKeys
     *
     * @param int   $ttl
     *
     * @return bool|int|string
     */
    public function mset ($dataByKeys, $ttl = self::DEFAULT_TTL)
    {
        $result = $this->_call('mSet', [$dataByKeys]);
        if ($result) {
            foreach (array_keys($dataByKeys) as $key) {
                $this->_call('expire', [$key, $ttl]);
            }
        }
        
        return $result;
    }

    /**
     * @param string $scope
     * @param array $keysArray
     * @return bool|int|string
     * @throws Exception|RedisException
     */
    public function hdelArray ($scope, $keysArray)
    {
        $args = $keysArray;
        array_unshift($args, $scope);

        return $this->_call('hDel', $args);
    }
    
    public function mget ($keys) 
    {
        $result = $this->_call('mGet', [$keys]);
        
        if (is_array($result)) {
            return array_combine($keys, $result);
        }
        
        return $result;   
    }

    /**
     * @param $scope
     * @return bool|int|string
     * @throws Exception|RedisException
     */
    public function hkeys ($scope)
    {
        return $this->_call(__FUNCTION__, [$scope]);
    }

    /**
     * @param string $scope
     * @param string $key
     * @return bool|int|string
     * @throws Exception|RedisException
     */
    public function hget ($scope, $key)
    {
        return $this->_call('hGet', [$scope, $key]);
    }

    public function hdel($key, $hashKey1, $hashKey2 = null, $hashKeyN = null)
    {
        return $this->_call('hDel', [$key, $hashKey1, $hashKey2, $hashKeyN]);
    }

    public function hincrby($key, $hashkey, $value = 1)
    {
        return $this->_call('hIncrBy', [$key, $hashkey, $value]);
    }

    /**
     * @return void
     * @throws RedisException
     */
    protected function reconnect()
    {
        try {
            $this->_init();
        } catch (RedisException $e) {
            if ($this->reconnectIterator > self::MAX_RECONNECT_TRY) {
                $this->reconnectIterator = 1;
                throw new $e;
            }

            sleep(self::RECONNECTS_TIMEOUT);
            $this->reconnectIterator++;
            $this->reconnect();
        }
    }

    /**
     * @param string $method
     * @param array $arguments
     * @return string|integer|boolean
     * @throws \RuntimeException|RedisException
     */
    protected function _call($method, array $arguments)
    {
        try {
            return call_user_func_array([$this->_instance, $method], $arguments);
        } catch (RedisException | Exception $e) { }

        $this->reconnect();

        try {
            return call_user_func_array([$this->_instance, $method], $arguments);
        } catch (\Exception $e) {
            throw new \RuntimeException('Redis '.$method.' exception: '.$e->getMessage());
        }
    }
}
