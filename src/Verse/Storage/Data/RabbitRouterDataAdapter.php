<?php


namespace Verse\Storage\Data;

use Mu\Env;
use Verse\Storage\Request\StorageDataRequest;

class RabbitRouterDataAdapter extends DataAdapterProto
{
    private $queueKey = 'address';
    
    /**
     * @return \Router\Router
     */
    public function getRouter () 
    {
        return Env::getRouter();
    }
    
    /**
     * @param $id         null|int|array
     * @param $insertBind array
     *
     * @return StorageDataRequest
     */
    public function getInsertRequest($id, $insertBind)
    {
        $self = $this;
        $primary = $this->primaryKey;
        
        return $request = new StorageDataRequest(
            [$id, $insertBind, $primary], 
            function ($id, $insertBind, $primary) use ($self) {
                $insertBind[$primary] = $id;
                $queue = $insertBind[$self->getQueueKey()] ?? $self->getResource();
                $self->getRouter()->publish($insertBind, $queue);
                return $insertBind;
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
        $self = $this;
        $primary = $this->primaryKey;
    
        return $request = new StorageDataRequest(
            [$insertBindsByKeys, $primary],
            function ($insertBindsByKeys, $primary) use ($self) {
                $router = $self->getRouter();
                $queueKey = $self->getQueueKey();
                $resource = $self->getResource();
                
                foreach ($insertBindsByKeys as $id => &$bind) {
                    $bind[$primary] = $id;
                    $queue = $bind[$queueKey] ?? $resource;
                    $router->publish($bind, $queue);    
                } unset($bind);
                
                return $insertBindsByKeys;
            }
        );
    }
    
    /**
     * @param $id int|array
     * @param $updateBind
     *
     * @return StorageDataRequest
     * @throws \Exception
     */
    public function getUpdateRequest($id, $updateBind)
    {
        throw new \Exception("Not Supported");
    }
    
    /**
     * @param $updateBindsByKeys
     *
     * @return StorageDataRequest
     * @throws \Exception
     */
    public function getBatchUpdateRequest($updateBindsByKeys)
    {
        throw new \Exception("Not Supported");
    }
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest fetch result is array [ 'key1' => ['primary' => 'key1', ...], ... ]
     * @throws \Exception
     */
    public function getReadRequest($ids)
    {
        throw new \Exception("Not Supported");
    }
    
    /**
     * @param $ids int|array
     *
     * @return StorageDataRequest
     */
    public function getDeleteRequest($ids)
    {
        throw new \Exception("Not Supported");
    }
    
    /**
     * @param array $filter
     *
     * @param int   $limit
     * @param array $conditions
     *
     * @return StorageDataRequest
     */
    public function getSearchRequest($filter, $limit = 1, $conditions = [])
    {
        $self = $this;
    
        return $request = new StorageDataRequest(
            [$limit, $conditions],
            function ($limit, $conditions) use ($self) {
                $timeout = $conditions['timeout'] ?? 1;
                $consumer = $self->getRouter()->getConsumer($self->getResource(), $timeout);
                $results = [];
                
                while ($limit--) {
                    $results[] = $consumer->readOne();
                }
            
                return $results;
            }
        );
    }
    
    /**
     * @return string
     */
    public function getQueueKey(): string
    {
        return $this->queueKey;
    }
    
    /**
     * @param string $queueKey
     */
    public function setQueueKey(string $queueKey)
    {
        $this->queueKey = $queueKey;
    }
    
}