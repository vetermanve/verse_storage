<?php


namespace Verse\Storage;


use Verse\Storage\Data\JBaseDataAdapter;
use Verse\Storage\Example\ExampleStorage;
use Verse\Storage\Spec\Compare;
use PHPUnit\Framework\TestCase;

class SimpleJsonStorageTest extends TestCase
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
    
    protected function setUp() : void
    {
        parent::setUp(); 
        $this->testDataDir = __DIR__.'/jbase';
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


    public function testUpdateManyBatchAndReadMget() : void
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
        
        $updateBinds = [];
        
        
        foreach ($insertDataItems as $id => $item) {
            $checkValue = microtime(1);
            $updateBinds[$id] = [
                'updated' => $checkValue
            ];

            $insertDataItems[$id]['updated'] = $checkValue; 
        }

        $writeResult = $storage->write()->updateBatch($updateBinds, __METHOD__);
        foreach ($writeResult as $result) {
            $this->assertTrue($result);
        }

        $checkDataResult = $storage->read()->mGet($ids, __METHOD__);
        $this->assertEquals($insertDataItems, $checkDataResult, 'get by id error');
    }

    public function testInsertManyUpdateOneByOneAndReadMget() : void
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


        foreach ($insertDataItems as $id => $item) {
            $checkValue = microtime(1);
            $updateBind = [
                'updated' => $checkValue
            ];
            
            $res = $storage->write()->update($id, $updateBind, __METHOD__);
            $this->assertNotEmpty($res);
                
            $insertDataItems[$id]['updated'] = $checkValue;
        }

        $checkDataResult = $storage->read()->mGet($ids, __METHOD__);
        $this->assertEquals($insertDataItems, $checkDataResult, 'get by id error');
    }

    public function testInsertManyAndSearchIdsIn() : void
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
        
        $filter = [
            ['id', Compare::IN, $ids]
        ];
        
        $checkDataResult = $storage->search()->find($filter, \count($ids), __METHOD__);
        $this->assertEquals($insertDataItems, $checkDataResult, 'get by id error');
    }

    public function testInsertManyAndSearchRandomIds() : void
    {
        $storage = $this->getStorage();

        $count = 10;

        $insertDataItems = [];
        
        $testSearch = [];

        while ($count--) {
            $id = $count . '.' . microtime(1);

            $testData = [
                'id'   => $id,
                'test' => microtime(1),
            ];

            $insertDataItems[$id] = $testData;
            
            if (random_int(0, 1)) {
                $testSearch[$id] = $testData;
            }
        }

        $writeResult = $storage->write()->insertBatch($insertDataItems, __METHOD__);
        $this->assertNotEmpty($writeResult);

        $ids = array_column($testSearch, 'id');

        $filter = [
            ['id', Compare::IN, $ids]
        ];

        $checkDataResult = $storage->search()->find($filter, \count($ids), __METHOD__);
        $this->assertEquals($testSearch, $checkDataResult, 'random serach by id error');
    }

    public function testInsertManyAndSearchOneByFindOne() : void
    {
        $storage = $this->getStorage();

        $count = 10;

        $insertDataItems = [];

        $testSearch = [];

        while ($count--) {
            $id = $count . '.' . microtime(1);

            $testData = [
                'id'   => $id,
                'test' => microtime(1),
            ];

            $insertDataItems[$id] = $testData;
        }
        
        $specialItem = [
            'id' => 'special',
            'test_special' => microtime(1),
        ]; 
            
        $insertDataItems[$specialItem['id']] = $specialItem;

        $writeResult = $storage->write()->insertBatch($insertDataItems, __METHOD__);
        $this->assertNotEmpty($writeResult);

        $ids = array_column($testSearch, 'id');

        $filter = [
            ['id', Compare::IN, [$specialItem['id']]]
        ];

        $checkDataResult = $storage->search()->findOne($filter, \count($ids), __METHOD__);
        $this->assertEquals($specialItem, $checkDataResult, 'random serach by id error');
    }
}