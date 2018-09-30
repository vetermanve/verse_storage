<?php


namespace Verse\Storage;


class StorageDependency
{
    const WRITE_MODULE  = 'write';
    const READ_MODULE   = 'read';
    const SEARCH_MODULE = 'search';
    const CACHE_MODULE  = 'cache';
    
    const PROFILER      = 'profiler';
    
    const DATA_ADAPTER  = 'data';
    
    private $modules = [];
    
    /**
     * @var StorageContext
     */
    private $context;
    
    /**
     * StorageDependency constructor.
     *
     * @param StorageContext $context
     */
    public function __construct(StorageContext $context)
    {
        $this->context = $context;
    }
    
    public function bootstrap($module, $required = true)
    {
        if (!isset($this->modules[$module])) {
            if ($required) {
                throw new \RuntimeException('Module '.$module.' not supported');
            }
            
            return null;
        }
        
        if (is_callable($this->modules[$module])) {
            $this->modules[$module] = $this->modules[$module]($this->context);
        }
        
        return $this->modules[$module];
    }
    
    public function setModule($moduleName, $module)
    {
        return $this->modules[$moduleName] = $module;
    }
}