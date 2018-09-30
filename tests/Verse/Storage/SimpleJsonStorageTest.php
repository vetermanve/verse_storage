<?php


namespace Verse\Storage;


use Verse\Storage\Data\JBaseDataAdapter;
use Verse\Storage\Example\ExampleStorage;

class SimpleJsonStorageTest extends \PHPUnit_Framework_TestCase
{
    protected $testDataDir;
    
    protected function clearDir ($dir) {
        if (is_dir($dir)) {
            $objects = scandir($dir, SCANDIR_SORT_NONE);
            foreach ($objects as $object) {
                if ($object !== "." && $object !== "..") {
                    if (filetype($dir."/".$object) === "dir"){
                        self::clearDir($dir."/".$object);
                    }else{
                        \unlink($dir."/".$object);
                    }
                }
            }
            reset($objects);
            rmdir($dir);
        }
    }
    
    protected function setUp()
    {
        parent::setUp(); 
        $this->testDataDir = __DIR__.'/data';
        if (file_exists($this->testDataDir)) {
            self::clearDir($this->testDataDir);    
        }
    }

    /**
     * @return StorageProto
     */
    private function getStorage() {
        $storage = new ExampleStorage();

        $adapter = new JBaseDataAdapter();
        // set data location
        $adapter->setDataRoot($this->testDataDir);
        // set database (folder) name
        $adapter->setDatabase('test-database');
        // set table (folder) name
        $adapter->setResource('test-table');
        
        $storage->getDiContainer()->setModule(StorageDependency::DATA_ADAPTER, $adapter);
        
        return $storage;
    }

    public function testWriteOneAndReadById () 
    {
        $storage = $this->getStorage();
        
        $id = microtime(1);
        
        $testData = [
            'id' => $id,
            'test' => microtime(1),
        ];
            
        $writeResult = $storage->write()->insert($id, $testData, __METHOD__);
        $this->assertNotEmpty($writeResult);
        
        $checkResultById = $storage->read()->get($id, __METHOD__);
        $this->assertEquals($testData, $checkResultById, 'get by id error');
    }

    public function testWriteOneAndReadByIds ()
    {
        $storage = $this->getStorage();

        $id = microtime(1);

        $testData = [
            'id' => $id,
            'test' => microtime(1),
        ];

        $writeResult = $storage->write()->insert($id, $testData, __METHOD__);
        $this->assertNotEmpty($writeResult);

        $checkResultById = $storage->read()->mGet([$id], __METHOD__);
        $this->assertEquals($testData, array_pop($checkResultById), 'mget by id error');
    }

    public function testWriteAndReadManyOneByOne()
    {
        $storage = $this->getStorage();

        $count = 5;
        
        $checkDataItems = [];
        
        while ($count--) {
            $id = $count.'.'.microtime(1);

            $testData = [
                'id' => $id,
                'test' => microtime(1),
            ];
            
            $checkDataItems[] = $testData;
            
            $writeResult = $storage->write()->insert($id, $testData, __METHOD__);
            $this->assertNotEmpty($writeResult);
        }
        
        foreach ($checkDataItems as $checkDataItem) {
            $checkDataResult = $storage->read()->get($checkDataItem['id'], __METHOD__);
            $this->assertEquals($checkDataItem, $checkDataResult, 'get by id error');    
        }
    }

    public function testWriteManyOneByOneAndReadMget()
    {
        $storage = $this->getStorage();

        $count = 10;

        $checkDataItems = [];

        while ($count--) {
            $id = $count . '.' . microtime(1);

            $testData = [
                'id'   => $id,
                'test' => microtime(1),
            ];

            $checkDataItems[] = $testData;

            $writeResult = $storage->write()->insert($id, $testData, __METHOD__);
            $this->assertNotEmpty($writeResult);
        }

        $ids = array_column($checkDataItems, 'id');
        $checkDataResult = $storage->read()->mGet($ids, __METHOD__);
        $this->assertEquals($checkDataItems, array_values($checkDataResult), 'get by id error');
    }

    public function testWriteManyBatchAndReadMget() : void
    {
        $storage = $this->getStorage();

        $count = 10;

        $insertDataItems = [];

        while ($count--) {
            $id = $count . '.' . microtime(1);

            $testData = [
                'id'   => $id,
                'test' => microtime(1),
            ];

            $insertDataItems[$id] = $testData;
        }

        $writeResult = $storage->write()->insertBatch($insertDataItems, __METHOD__);
        $this->assertNotEmpty($writeResult);

        $ids = array_column($insertDataItems, 'id');
        $checkDataResult = $storage->read()->mGet($ids, __METHOD__);
        $this->assertEquals($insertDataItems, $checkDataResult, 'get by id error');
    }
}