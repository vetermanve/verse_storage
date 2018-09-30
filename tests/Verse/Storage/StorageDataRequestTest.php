<?php


namespace Verse\Storage;


use PHPUnit\Framework\TestCase;
use Verse\Storage\Request\StorageDataRequest;

class StorageDataRequestTest extends TestCase
{
    public function testCreation () 
    {
        $a = 1;
        $b = 10;
        $request = new StorageDataRequest(
            [$a, $b], 
            function ($a, $b) {
                $cont = new \stdClass();
                $cont->a = $a;
                $cont->b = $b;
                return $cont;
            },
            function (\stdClass $cont) {
                return $cont->a + $cont->b;
            }
        );
        
        $request->send();
        $request->fetch();
        
        $this->assertEquals($a + $b, $request->getResult());
    }

    public function testAutoFetch ()
    {
        $a = 1;
        $b = 10;
        $request = new StorageDataRequest(
            [$a, $b],
            function ($a, $b) {
                $cont = new \stdClass();
                $cont->a = $a;
                $cont->b = $b;
                return $cont;
            },
            function (\stdClass $cont) {
                return $cont->a + $cont->b;
            }
        );

        $request->send();

        $this->assertEquals($a + $b, $request->getResult());
    }

    public function testAutoSendAndFetch ()
    {
        $a = 1;
        $b = 10;
        $request = new StorageDataRequest(
            [$a, $b],
            function ($a, $b) {
                $cont = new \stdClass();
                $cont->a = $a;
                $cont->b = $b;
                return $cont;
            },
            function (\stdClass $cont) {
                return $cont->a + $cont->b;
            }
        );
        
        $exception = null;
        try  {
            $request->getResult();
        } catch (\Throwable $throwable) {
            $exception = $throwable;
        }
        
        $this->assertInstanceOf(\Throwable::class, $exception);
        
        $request->send();
        $this->assertEquals($a + $b, $request->getResult());
    }
}