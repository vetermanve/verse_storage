<?php


namespace Verse\Storage\ReadModule;


use Verse\Storage\StorageDataAccessModuleProto;

class SimpleReadModule extends StorageDataAccessModuleProto implements ReadModuleInterface
{
    public function get($id, $caller, $default = null)
    {
        $t = $this->profiler->openTimer(__METHOD__, $id, $caller);
        $request = $this->dataAdapter->getReadRequest([$id]);
        $request->send();
        $result = $request->fetch();
        $this->profiler->finishTimer($t);
        return $result ? reset($result) : $default;
    }
    
    /**
     * @param       $ids
     * @param       $caller
     * @param array $default
     *
     * @return array
     */
    public function mGet($ids, $caller, $default = [])
    {
        $timer = $this->profiler->openTimer(__METHOD__, $ids, $caller);
        $request = $this->dataAdapter->getReadRequest($ids);
        $request->send();
        $request->fetch();
        $this->profiler->finishTimer($timer);
        
        return $request->hasResult() ? $request->getResult() : $default;
    }
}