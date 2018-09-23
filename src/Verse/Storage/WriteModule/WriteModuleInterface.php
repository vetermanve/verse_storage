<?php


namespace Verse\Storage\WriteModule;


interface WriteModuleInterface
{
    public function configure();
    
    public function insert($id, $bind, $callerMethod);
    public function remove($id, $callerMethod);
    public function update ($id, $bind, $callerMethod);
    public function insertBatch($bindsByIds, $callerMethod);
    public function updateBatch($bindsByIds, $callerMethod);
}