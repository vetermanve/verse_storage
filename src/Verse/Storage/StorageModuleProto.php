<?php


namespace Verse\Storage;


class StorageModuleProto
{
    /**
     * @var StorageDependency
     */
    protected $diContainer;
    
    /**
     * @var StorageContext
     */
    protected $context;
    
    /**
     * @return StorageDependency
     */
    public function getDiContainer()
    {
        return $this->diContainer;
    }
    
    /**
     * @param StorageDependency $diContainer
     */
    public function setDiContainer($diContainer)
    {
        $this->diContainer = $diContainer;
    }
    
    /**
     * @return StorageContext
     */
    public function getContext()
    {
        return $this->context;
    }
    
    /**
     * @param StorageContext $context
     */
    public function setContext($context)
    {
        $this->context = $context;
    }
    
    
}