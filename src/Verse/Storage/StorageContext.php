<?php


namespace Verse\Storage;


use Verse\Modular\ModularContextProto;

class StorageContext extends ModularContextProto
{
    const RESOURCE = 'resource';
    const DATABASE = 'database';
    const TYPE = 'type';
    const SCOPE = 'scope';
    
    const TIMEOUT = 'timeout';
}