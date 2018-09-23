<?php


namespace Verse\Storage;


class StorageProfiler
{
    const START_TIME  = 'start';
    const FINISH_TIME = 'finish';
    const FULL_TIME   = 'time';
    const METHOD      = 'method';
    const CALLER      = 'caller';
    const META        = 'meta';
    
    private $timers = [];
    
    private $timerIdSeq = 0;
    
    private $timerReportCallback;
    
    public function openTimer($method, $meta, $caller)
    {
        $timerId = $this->_getNextTimerId();
    
        $this->timers[$timerId] = [
            self::START_TIME => round(microtime(1), 6),
            self::FULL_TIME  => 0,
            self::METHOD     => $method,
            self::META       => $meta,
            self::CALLER     => $caller,
        ];
        
        return $timerId;
    }
    
    public function finishTimer($timerId)
    {
        $this->timers[$timerId][self::FULL_TIME] = round(microtime(1) - $this->timers[$timerId][self::START_TIME], 6);
        unset($this->timers[$timerId][self::START_TIME]);
        if ($this->timerReportCallback) {
            call_user_func($this->timerReportCallback, $this->timers[$timerId]); 
        }
    }
    
    private function _getNextTimerId()
    {
        if ($this->timerIdSeq = PHP_INT_MAX) {
            $this->timerIdSeq = 0;
        }
        
        return $this->timerIdSeq++;
    }
    
    /**
     * @return array
     */
    public function getTimers()
    {
        return $this->timers;
    }
    
    /**
     * @return mixed
     */
    public function getTimerReportCallback()
    {
        return $this->timerReportCallback;
    }
    
    /**
     * @param mixed $timerReportCallback
     */
    public function setTimerReportCallback($timerReportCallback)
    {
        $this->timerReportCallback = $timerReportCallback;
    }
    
}