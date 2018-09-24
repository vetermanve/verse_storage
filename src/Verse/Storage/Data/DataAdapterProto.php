<?php


namespace Verse\Storage\Data;


abstract class DataAdapterProto implements DataAdapterInterface
{
    /**
     * @var string
     */
    protected $primaryKey = 'id';
    
    /**
     * @var string
     */
    protected $resource;
    
    /**
     * @return string
     */
    public function getPrimaryKey()
    {
        return $this->primaryKey;
    }
    
    /**
     * @param string $primaryKey
     */
    public function setPrimaryKey($primaryKey)
    {
        $this->primaryKey = $primaryKey;
    }
    
    
    /**
     * @return string
     */
    public function getResource()
    {
        return $this->resource;
    }
    
    /**
     * @param string $resource
     */
    public function setResource($resource)
    {
        $this->resource = $resource;
    }
    
}