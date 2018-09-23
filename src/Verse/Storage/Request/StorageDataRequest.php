<?php


namespace Verse\Storage\Request;


class StorageDataRequest
{
    /**
     * @var callable
     */
    protected $sendFunction;
    
    /**
     * @var callable
     */
    protected $fetchFunction;
    
    /**
     * @var array
     */
    protected $arguments = [];
    
    /**
     * @var bool
     */
    protected $wasSent = false;
    
    /**
     * @var bool
     */
    protected $wasFetched = false;
    
    /**
     * @var mixed
     */
    protected $fetchResult = null;
    
    /**
     * @var mixed
     */
    protected $sendResult = null;
    
    /**
     * StorageDataRequest constructor.
     *
     * @param callable $sendFunction
     * @param callable $fetchFunction
     * @param array    $arguments
     */
    public function __construct(array $arguments = [], $sendFunction = null, $fetchFunction = null)
    {
        $this->bind($arguments, $sendFunction, $fetchFunction);
    }
    
    public function bind($arguments, $sendFunction, $fetchFunction = null)
    {
        $this->arguments     = $arguments;
        $this->sendFunction  = $sendFunction;
        $this->fetchFunction = $fetchFunction;
    }
    
    /**
     * @return mixed|null
     */
    public function send()
    {
        if (!$this->wasSent && is_callable($this->sendFunction)) {
            $this->sendResult = call_user_func_array($this->sendFunction, $this->arguments);
            $this->wasSent = true;
        }
        
        return $this->sendResult;
    }
    
    /**
     * @return mixed|null
     */
    public function fetch()
    {
        // single function data request
        if (!$this->fetchFunction) { 
            return $this->sendResult; 
        }
        
        if (!$this->wasFetched) {
            $this->fetchResult = call_user_func_array(
                $this->fetchFunction, 
                is_array($this->sendResult) ? $this->sendResult : [$this->sendResult] 
            );
            $this->wasFetched  = true;
        }
        
        return $this->fetchResult;
    }
    
    /**
     * @return array|null|bool
     */
    public function getResult () 
    {
        return $this->wasFetched ? $this->fetchResult : $this->fetch();
    }
    
    public function hasResult () 
    {
        return $this->wasFetched ? $this->fetchResult !== null : $this->fetch() !== null;
    }
}