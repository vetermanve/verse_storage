<?php


namespace Verse\Storage\Data;


use Verse\Storage\Request\StorageDataRequest;

interface DataAdapterInterface
{
    /**
     * @param $id null|int|array
     * @param $insertBind array
     *
     * @return StorageDataRequest
     */
    public function getInsertRequest ($id, $insertBind);
    
    /**
     * @param $insertBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchInsertRequest ($insertBindsByKeys);
    
    /**
     * @param $id int|array
     * @param $updateBind
     *
     * @return StorageDataRequest
     */
    public function getUpdateRequest ($id, $updateBind);
    
    /**
     * @param $updateBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchUpdateRequest ($updateBindsByKeys);
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest fetch result is array [ 'key1' => ['primary' => 'key1', ...], ... ]
     */
    public function getReadRequest ($ids);
    
    /**
     * @param $ids int|array
     *
     * @return StorageDataRequest
     */
    public function getDeleteRequest ($ids);
    
    /**
     * @param array $filter
     *
     * @param int   $limit
     * @param array $conditions
     *
     * @return StorageDataRequest
     */
    public function getSearchRequest ($filter, $limit = 1, $conditions = []);
}