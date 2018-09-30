<?php


namespace Verse\Storage;


use PHPUnit\Framework\TestCase;

class StorageDependencyTest extends TestCase
{
    public function testModuleSet ()
    {
        $di = new StorageDependency(new StorageContext());
        $module = new \stdClass();
        $module->hasFuntions = true;
        $di->setModule('test-module', $module);
        $this->assertEquals($module, $di->bootstrap('test-module'));
    }
    
    public function testModuleBoot () 
    {
        $di = new StorageDependency(new StorageContext());
        $module = new \stdClass();
        $module->hasFuntions = true;
        $di->setModule('test-module', function () use ($module) {
             return $module;
        });
        
        $this->assertEquals($module, $di->bootstrap('test-module'));
    }

    public function testModuleNotFundNotRequired ()
    {
        $di = new StorageDependency(new StorageContext());
        $this->assertNull($di->bootstrap('missing-module', false));
    }

    public function testModuleNotFundRequiredAndDropException ()
    {
        $di = new StorageDependency(new StorageContext());
        $exceptionObject = null;
        $moduleName = 'missing-module';
        
        try {
            $di->bootstrap($moduleName);    
        } catch (\RuntimeException $exception) {
            $exceptionObject = $exception;  
        }
        
        $this->assertInstanceOf(\RuntimeException::class, $exceptionObject);
        $this->assertEquals('Module '.$moduleName.' not supported', $exceptionObject->getMessage());
    }
}