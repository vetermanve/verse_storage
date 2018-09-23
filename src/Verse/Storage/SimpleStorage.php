<?php


namespace Verse\Storage;


use Verse\Storage\ReadModule\SimpleReadModule;
use Verse\Storage\SearchModule\SimpleSearchModule;
use Verse\Storage\WriteModule\SimpleWriteModule;

/**
 * Class SimpleStorage
 * 
 * @package App\Rest\Auth\Storage
 * @method SimpleReadModule read()
 * @method SimpleWriteModule write()
 * @method SimpleSearchModule search()
 */
abstract class SimpleStorage extends StorageProto
{
    public function setupDi()
    {
        $context = $this->context;
        $container = $this->diContainer;
        
        $container->setModule(StorageDependency::WRITE_MODULE, function () use ($container, $context) {
            $module = new SimpleWriteModule();
            $module->setDiContainer($container);
            $module->setContext($context);
            $module->configure();
            return $module;
        });
    
        $container->setModule(StorageDependency::READ_MODULE, function () use ($container, $context) {
            $module = new SimpleReadModule();
            $module->setDiContainer($container);
            $module->setContext($context);
            $module->configure();
            return $module;
        });
    
        $container->setModule(StorageDependency::SEARCH_MODULE, function () use ($container, $context) {
            $module = new SimpleSearchModule();
            $module->setDiContainer($container);
            $module->setContext($context);
            $module->configure();
            return $module;
        });
    }
}