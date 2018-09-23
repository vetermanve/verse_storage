<?php


namespace Verse\Storage;


use Verse\Storage\Data\DataAdapterInterface;

class StorageDataAccessModuleProto extends StorageModuleProto
{
    /**
     * @var DataAdapterInterface
     */
    protected $dataAdapter;
    
    /**
     * @var StorageProfiler
     */
    protected $profiler;
    
    public function configure ()
    {
        $this->dataAdapter = $this->diContainer->bootstrap(StorageDependency::DATA_ADAPTER);
        $this->profiler = $this->diContainer->bootstrap(StorageDependency::PROFILER);
    }
}