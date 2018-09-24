<?php


namespace Verse\Storage\SearchModule;


interface SearchModuleInterface
{
    public function find($filters, $limit, $caller, $meta = []);
    public function findOne($filters, $caller, $meta = []);
}