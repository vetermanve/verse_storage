<?php


namespace Verse\Storage\WriteModule;


use Verse\Storage\StorageDataAccessModuleProto;

class SimpleWriteModule extends StorageDataAccessModuleProto implements WriteModuleInterface
{
    /**
     * @param $bind
     * @param $callerMethod
     *
     * @return array|bool|null
     */
    public function insert ($id, $bind, $callerMethod) 
    {
        $timer = $this->profiler->openTimer(__METHOD__, $bind, $callerMethod);
        $request = $this->dataAdapter->getInsertRequest($id, $bind);
        $request->send();
        $request->fetch();
        $this->profiler->finishTimer($timer);
        
        return $request->getResult();
    }
    
    public function insertBatch($bindsByIds, $callerMethod)
    {
        $timer = $this->profiler->openTimer(__METHOD__, $bindsByIds, $callerMethod);
        $request = $this->dataAdapter->getBatchInsertRequest($bindsByIds);
        $request->send();
        $request->fetch();
        $this->profiler->finishTimer($timer);
    
        return $request->getResult();
    }
    
    public function update ($id, $bind, $callerMethod) 
    {
        $timer = $this->profiler->openTimer(__METHOD__, $bind, $callerMethod);
        $request = $this->dataAdapter->getUpdateRequest($id, $bind);
        $request->send();
        $request->fetch();
        $this->profiler->finishTimer($timer);
    
        return $request->getResult();
    }
    
    public function updateBatch($bindsByIds, $callerMethod)
    {
        $timer = $this->profiler->openTimer(__METHOD__, $bind, $callerMethod);
        $request = $this->dataAdapter->getUpdateRequest($id, $bind);
        $request->send();
        $request->fetch();
        $this->profiler->finishTimer($timer);
    }
    
    public function remove($id, $callerMethod)
    {
        $timer = $this->profiler->openTimer(__METHOD__, '', $callerMethod);
        $request = $this->dataAdapter->getDeleteRequest([$id]);
        $request->send();
        $request->fetch();
        $this->profiler->finishTimer($timer);
    
        return $request->getResult();
    }
}