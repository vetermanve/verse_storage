<?php


namespace Verse\Storage\Data;


use Verse\Storage\Request\StorageDataRequest;

class CacheFlowStorageDataAdapter extends DataAdapterProto
{
    /**
     * @var DataAdapterInterface
     */
    protected $cacheDataAdapter;
    
    /**
     * @var DataAdapterInterface;
     */
    protected $sourceDataAdapter;
    
    /**
     * CacheFlowStorageDataAdapter constructor.
     *
     * @param DataAdapterInterface $cacheDataAdapter
     * @param DataAdapterInterface $sourceDataAdapter
     */
    public function __construct(DataAdapterInterface $cacheDataAdapter, DataAdapterInterface $sourceDataAdapter)
    {
        $this->cacheDataAdapter  = $cacheDataAdapter;
        $this->sourceDataAdapter = $sourceDataAdapter;
    }
    
    public function getBatchInsertRequest($insertBindsByKeys)
    {
        return new StorageDataRequest(
            [$insertBindsByKeys, $this->sourceDataAdapter, $this->cacheDataAdapter],
            function ($insertBindsByKeys, DataAdapterInterface $source, DataAdapterInterface $cache) {
                $request = $source->getBatchInsertRequest($insertBindsByKeys);
                $request->send();
                $request->fetch();
                
                $cache->getDeleteRequest(array_keys($insertBindsByKeys))->send();
                
                return $request->getResult();
            }
        );
    }
    
    /**
     * @param $id         null|int|array
     * @param $insertBind array
     *
     * @return StorageDataRequest
     */
    public function getInsertRequest($id, $insertBind)
    {
        return $this->getBatchInsertRequest([$id => $insertBind]);
    }
    
    /**
     * @param $id int|array
     * @param $updateBind
     *
     * @return StorageDataRequest
     */
    public function getUpdateRequest($id, $updateBind)
    {
        return new StorageDataRequest(
            [$this->sourceDataAdapter, $this->cacheDataAdapter, $id, $updateBind],
            function (DataAdapterInterface $sourceData, DataAdapterInterface $cacheData, $id, $updateBind) {
                $request = $sourceData->getUpdateRequest($id, $updateBind);
                $request->send();
                $result = $request->fetch();
                if ($result) {
                    $cacheData->getDeleteRequest($id)->send();
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
        return new StorageDataRequest(
            [$this->sourceDataAdapter, $this->cacheDataAdapter, $updateBindsByKeys],
            function (DataAdapterInterface $sourceData, DataAdapterInterface $cacheData, $updateBindsByKeys) {
                $request = $sourceData->getBatchUpdateRequest($updateBindsByKeys);
                $request->send();
                $result = $request->fetch();
                if ($result) {
                    $cacheData->getDeleteRequest(array_keys($updateBindsByKeys))->send();
                }
            
                return $result;
            }
        );
    }
    
    /**
     * @param $ids int|array
     *
     * @return StorageDataRequest
     */
    public function getDeleteRequest($ids)
    {
        return new StorageDataRequest(
            [$this->sourceDataAdapter, $this->cacheDataAdapter, $ids],
            function (DataAdapterInterface $sourceData, DataAdapterInterface $cacheData, $ids) {
                $request = $sourceData->getDeleteRequest($ids);
                $request->send();
                $result = $request->fetch();
                $cacheData->getDeleteRequest($ids)->send();
            
                return $result;
            }
        );
    }
    
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest
     */
    public function getReadRequest($ids)
    {
        $cacheDataAdapter = $this->cacheDataAdapter; 
        $primaryKey = $this->primaryKey;
        return new StorageDataRequest(
            [$this->sourceDataAdapter, $this->cacheDataAdapter, $ids],
            function (DataAdapterInterface $sourceData, DataAdapterInterface $cacheData, $ids) {
                $request = $cacheData->getReadRequest($ids);
                $request->send();
                
                $cacheReadResult = $request->fetch();
                
                $cacheReadSuccess = count($cacheReadResult) == count($ids);
                if (!$cacheReadSuccess) {
                    $request = $sourceData->getReadRequest($ids);
                    $request->send();
                }
            
                return [$request, $cacheReadSuccess];
            },
            function (StorageDataRequest $request, $cacheReadSuccess) use ($cacheDataAdapter, $primaryKey) {
                $result = $request->fetch();
                if ($result && !$cacheReadSuccess) {
                    $cacheDataAdapter->getBatchInsertRequest($result)->send();
                }
                
                return $result;
            }
        );
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
        
        return $this->sourceDataAdapter->getSearchRequest($filter, $limit, $conditions);
    }
    
    /**
     * @return DataAdapterInterface
     */
    public function getCacheDataAdapter()
    {
        return $this->cacheDataAdapter;
    }
    
    /**
     * @param DataAdapterInterface $cacheDataAdapter
     */
    public function setCacheDataAdapter($cacheDataAdapter)
    {
        $this->cacheDataAdapter = $cacheDataAdapter;
    }
    
    /**
     * @return DataAdapterInterface
     */
    public function getSourceDataAdapter()
    {
        return $this->sourceDataAdapter;
    }
    
    /**
     * @param DataAdapterInterface $sourceDataAdapter
     */
    public function setSourceDataAdapter($sourceDataAdapter)
    {
        $this->sourceDataAdapter = $sourceDataAdapter;
    }
}