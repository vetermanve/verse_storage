<?php


namespace Verse\Storage\ReadModule;


use Verse\Storage\Request\StorageDataRequest;

interface ReadModuleInterface
{
    /**
     * @param      $id
     * @param      $caller
     * @param null $default
     *
     * @return mixed
     */
    public function get ($id, $caller, $default = null);
    
    
    /**
     * @param       $ids
     * @param       $caller
     * @param array $default
     *
     * @return array
     */
    public function mGet ($ids, $caller, $default = []);
    
//    public function getScope ($scope, $id, $caller, $default = null);
//    public function mgetScope ($scope, $ids, $caller, $default = []);
}