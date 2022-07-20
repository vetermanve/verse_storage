<?php

namespace Verse\Storage\DataAdaptersTests;

use PHPUnit\Framework\TestCase;
use Verse\Di\Env;
use Verse\Storage\Connector\Redis;
use Verse\Storage\Data\DataAdapterProto;
use Verse\Storage\Data\RedisCacheDataAdapter;
use Verse\Storage\Spec\Compare;

class RedisAdapterTest extends TestCase
{
    private static $resource = '';

    protected function setUp() : void
    {
        parent::setUp();
    }



    public static function setUpBeforeClass() : void
    {
        parent::setUpBeforeClass();
        Env::getContainer()->setModule(Redis::class, function () {
            return new Redis(['port' => 16379, ]);
        });
    }

    /**
     * @return DataAdapterProto
     */
    public function getAdapter() : DataAdapterProto
    {
        $adapter = new RedisCacheDataAdapter('test'.microtime(1));
        return $adapter;
    }

    public function testInsertRequest()
    {
        $adapter = $this->getAdapter();

        $request = $adapter->getInsertRequest(null, [
            'a' => 1,
            'b' => 'asdf',
            'c' => ';DROP TABLE TEST;',
        ]);

        $request->send();
        $request->fetch();

        $this->assertNotEmpty($request->getResult());
        $a = $request->getResult();
//        $adapter->getDeleteRequest();
    }

    public function testInsertBatchRequest()
    {
        $adapter = $this->getAdapter();

        $request = $adapter->getBatchInsertRequest([
            [
                'a' => time(),
                'b' => 'first_batch_insert',
                'c' => 'some_test big text;',
            ],
            [
                'c' => '\');DROP TABLE TEST;\'((',
                'b' => 'second_batch_insert',
                'a' => time(),
            ],
            [
                'b' => 'second_batch_insert',
                'c' => [
                    'd' => ['b\'la', 'b"la', 'b\\la'],
                    'e' => 1.2345345,
                    'f' => 'asdasdf',
                ],
                'a' => time(),
            ],
        ]);

        $request->send();
        $request->fetch();

        $this->assertNotEmpty($request->getResult());
    }


    public function testInsertFailsOnExistingRecord()
    {
        $adapter = $this->getAdapter();
//        $adapter->clearTable();


        $value1 = 100;
        ///
        $request1 = $adapter->getInsertRequest(1, [
            'value' => $value1,
        ]);

        $request1->send();
        $request1->fetch();

        $this->assertNotEmpty($request1->getResult());

        $value2 = 200;
        ///
        $request2 = $adapter->getInsertRequest(1, [
            'value' => $value2,
        ]);

        $request2->send();
        $request2->fetch();

        $this->assertEmpty($request2->getResult());
    }

    public function testInsertBatchDoNoInsertedOnConflictRequest()
    {
        $adapter = $this->getAdapter();

        //
        $request1 = $adapter->getInsertRequest(1, ['value' => 1]);
        $request1->send();
        $request1->fetch();

        //
        $this->assertNotEmpty($request1->getResult());

        $request2 = $adapter->getInsertRequest(1, ['value' => 2]);
        $request2->send();
        $request2->fetch();

        $this->assertEmpty($request2->getResult());

        $request3 = $adapter->getReadRequest([1]);
        $request3->send();
        $request3->fetch();
        $r = $request3->getResult();
        $this->assertEquals(1, $request3->getResult()[1]['value']);

        $requestBatch = $adapter->getBatchInsertRequest([
            1 => [
                'value' => 1,
            ],
            2 => [
                'value' => 100,
            ],
        ]);

        $requestBatch->send();
        $requestBatch->fetch();

        $checkArray = [
            1 => false,
            2 => [
                'id' => 2,
                'value' => 100,
            ],
        ];
            
        $this->assertNotEmpty($requestBatch->getResult());
        $this->assertEquals($checkArray, $requestBatch->getResult());
    }
    
    public function testUpdateRequest () 
    {
        $adapter = $this->getAdapter();

        $request1 = $adapter->getInsertRequest(1, ['value' => 1]);
        $request1->send();
        $request1->fetch();

        //
        $this->assertNotEmpty($request1->getResult());

        $request2 = $adapter->getUpdateRequest(1, ['value' => 2]);
        $request2->send();
        $request2->fetch();

        $this->assertNotEmpty($request2->getResult());
        $this->assertEquals(2, $request2->getResult()['value']);
    }

    public function testReadComplexPrimaryRequest()
    {
        $adapter = $this->getAdapter();

        $id1 = [1, 1];
        $id2 = [1, 100];
        
        //
        $request1 = $adapter->getInsertRequest($id1, ['value' => \array_sum($id1)]);
        $request1->send();
        $request1->fetch();

        $res1 = $request1->getResult();
        //
        $this->assertNotEmpty($res1);
        
        $request2 = $adapter->getInsertRequest($id2, ['value' => \array_sum($id2)]);
        $request2->send();
        $request2->fetch();

        $res2 = $request2->getResult();
        //
        $this->assertNotEmpty($res2);
        ///
        
        $requestRead = $adapter->getReadRequest([
            $id1,
            $id2
        ]);

        $requestRead->send();
        $requestRead->fetch();
        
        $testData = [
            $res1,
            $res2,
        ];
        
        $this->assertEquals($testData, array_values($requestRead->getResult()));
    }

    public function testReadSimplePrimaryRequest()
    {
        $adapter = $this->getAdapter();

        $data = [];
        foreach (range(10, 20) as $value) {
            $data[$value] = [
                'value' => $value*3,
            ];
        }
        
        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $res1 = $request1->getResult();
        $this->assertNotEmpty($res1);

        //
        $requestRead = $adapter->getReadRequest(array_keys($data));

        $requestRead->send();
        $requestRead->fetch();

        $this->assertCount(11, $requestRead->getResult());
        $this->assertEquals($res1, $requestRead->getResult());
    }


    public function testSearchRequestEQ () 
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();

        $data = [];
        foreach (range(10, 20) as $value) {
            $data[$value] = [
                'value' => $value*3,
            ];
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $res1 = $request1->getResult();
        $this->assertNotEmpty($res1);

        //
        $expected = [
            'id' => 20,
            'value' => 60,
        ];
        
        $requestRead = $adapter->getSearchRequest([
            ['value', Compare::EQ, $expected['value']]
        ]);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertNotEmpty($results);
        $this->assertCount(1, $results);
        $this->assertEquals($expected, \reset($results));
    }

    public function testSearchRequestNOTEQ ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        foreach (range(10, 20) as $value) {
            $data[$value] = [
                'value' => $value*3,
            ];
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $res1 = $request1->getResult();
        $this->assertNotEmpty($res1);

        //
        $remove = [
            'id' => 20,
            'value' => 60,
        ];
        
        unset($res1[array_search($remove, $res1)]);

        $requestRead = $adapter->getSearchRequest([
            ['value', Compare::NOT_EQ, $remove['value']]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertCount(10, $results);
        $this->assertEquals($res1, $results);
    }

    public function testSearchRequestNotEqOrEmpty ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        foreach (range(10, 20) as $value) {
            $data[$value] = [
                'value' => $value*3 %20 === 0 ? null : $value*3
            ];
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $res1 = $request1->getResult();
        $this->assertNotEmpty($res1);

        //
        $remove = [
            'id' => 11,
            'value' => 33,
        ];

        unset($res1[array_search($remove, $res1)]);

        $requestRead = $adapter->getSearchRequest([
            ['value', Compare::EMPTY_OR_NOT_EQ, $remove['value']]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertCount(10, $results);
        $this->assertEquals(array_values($res1), array_values($results));
    }

    public function testSearchRequestEqOrEmpty ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        $testData = [];
        $testVal = 110;
        foreach (range(10, 20) as $value) {
            $val = $value*10 %20 === 0 ? null : $value*10;
            $data[$value] = [
                'value' => $value*10 %20 === 0 ? null : $value*10
            ];
            
            if ($val === null || $val === $testVal) {
                $testData[] = [
                    'id' => $value,
                    'value' => $val,
                ];
            }
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $dataInsert = $request1->getResult();
        $this->assertCount(11, $dataInsert);

        $requestRead = $adapter->getSearchRequest([
            ['value', Compare::EMPTY_OR_EQ, $testVal]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertCount(7, $results);
        $this->assertEquals(array_values($testData), array_values($results));
    }

    public function testSearchRequestIn ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        $testData = [];
        $testVal = [110, 200];
        
        foreach (range(10, 20) as $value) {
            $val = $value*10;
            $data[$value] = [
                'value' => $val
            ];

            if (in_array($val, $testVal, true)) {
                $testData[] = [
                    'id' => $value,
                    'value' => $val,
                ];
            }
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $dataInsert = $request1->getResult();
        $this->assertCount(11, $dataInsert);

        $requestRead = $adapter->getSearchRequest([
            ['value', Compare::IN, $testVal]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertCount(2, $results);
        $this->assertEquals(array_values($testData), array_values($results));
    }

    public function testSearchRequestDoubleInIntersect ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        $testData = [];
        $testVal = [110, 200];

        foreach (range(10, 20) as $id) {
            $val = $id*10;
            $data[$id] = [
                'value' => $val
            ];

            if (in_array($val, $testVal, true)) {
                $testData[$id] = [
                    'id' => $id,
                    'value' => $val,
                ];
            }
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $dataInsert = $request1->getResult();
        $this->assertCount(11, $dataInsert);

        $requestRead = $adapter->getSearchRequest([
            ['id', Compare::IN, array_keys($testData)],
            ['value', Compare::IN, $testVal]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertCount(2, $results);
        $this->assertEquals(array_values($testData), array_values($results));
    }


    public function testSearchRequestDoubleInNotIntersect ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        $testData = [];
        $testVal = [110, 200];

        foreach (range(10, 20) as $id) {
            $val = $id*10;
            $data[$id] = [
                'value' => $val
            ];

            if (in_array($val, $testVal, true)) {
                $testData[$id] = [
                    'id' => $id,
                    'value' => $val,
                ];
            }
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $dataInsert = $request1->getResult();
        $this->assertCount(11, $dataInsert);

        $requestRead = $adapter->getSearchRequest([
            ['id', Compare::IN, array_keys($testData)],
            ['value', Compare::IN, [1,2]]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertCount(0, $results);
    }

    public function testSearchRequesEmptyIn ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test_constraint');
        $adapter->setPrimaryKey('id');
        $adapter->clearTable();

        $data = [];
        $testData = [];
        $testVal = [110, 200];

        foreach (range(10, 20) as $id) {
            $val = $id*10;
            $data[$id] = [
                'value' => $val
            ];

            if (in_array($val, $testVal, true)) {
                $testData[$id] = [
                    'id' => $id,
                    'value' => $val,
                ];
            }
        }

        //
        $request1 = $adapter->getBatchInsertRequest($data);
        $request1->send();
        $request1->fetch();

        $dataInsert = $request1->getResult();
        $this->assertCount(11, $dataInsert);

        $requestRead = $adapter->getSearchRequest([
            ['value', Compare::IN, []]
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertIsArray($results);
        $this->assertCount(0, $results);
    }


    public function testSearchRequesStrings ()
    {
        $this->markTestSkipped('Search not implemented');
        $adapter = $this->getAdapter();
        $adapter->setResource(self::$resource);
        $adapter->setTable('test');
        $adapter->clearTable();
        
        $testData = [
            [
                'a' => time(),
                'b' => 'first_batch_END',
                'c' => 'some_test big text;',
            ],
            [
                'a' => time(),
                'b' => 'second_batch_END',
                'c' => 'START ........ ..asd""" ',
            ],
            [
                'a' => time(),
                'b' => 'second_batch_ENDY',
                'c' => 'START!!!!|||||\s\s\s',
            ],
        ];

        $request = $adapter->getBatchInsertRequest($testData);

        $request->send();
        $request->fetch();

        $requestRead = $adapter->getSearchRequest([
            ['b', Compare::STR_ENDS, 'END'],
            ['c', Compare::STR_BEGINS, 'START']
        ], 100);

        $requestRead->send();
        $requestRead->fetch();

        $results = $requestRead->getResult();
        $this->assertIsArray($results);
        $this->assertCount(1, $results);
        
        $expected = [
            $testData[1] + ['id' => 1] 
        ];
        
        $this->assertEquals($expected, $results);
    }

}