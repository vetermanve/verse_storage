<?php


namespace Verse\Storage\WriteModule;


use Verse\Storage\Request\StorageDataRequest;
use Verse\Storage\StorageDataAccessModuleProto;

class AsyncWriteModule  extends StorageDataAccessModuleProto implements WriteModuleInterface
{
    /**
     * @param $id
     * @param $bind
     * @param $callerMethod
     *
     * @return StorageDataRequest
     */
    public function insert($id, $bind, $callerMethod)
    {
        $timer = $this->profiler->openTimer(__METHOD__, $bind, $callerMethod);
        $request = $this->dataAdapter->getInsertRequest($id, $bind);
        $request->send();
        $this->profiler->finishTimer($timer);
        
        return $request;
    }
    
    public function remove($id, $callerMethod)
    {
        throw new \Exception("NIY");
    }
    
    public function update($id, $bind, $callerMethod)
    {
        throw new \Exception("NIY");
    }
}