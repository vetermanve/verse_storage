<?php


namespace Verse\Storage\Data;


use Mu\Rpc\DataRequest;
use Verse\Storage\Request\StorageDataRequest;

class SynthesizeUniverseDataAdapter extends DataAdapterProto
{
    protected $module;
    
    protected $controller;
    
    protected $model;
    
    protected $timeout = 5;
    
    protected $type;
    
    protected $service;
    
    /**
     * @return DataRequest
     */
    protected function getRequest($name) {
    
        $request = new DataRequest();

        $request
            ->setModule($this->module)
            ->setController($this->controller)
            ->setMethod($name.$this->model)
            ->setTimeout($this->timeout)
        ;

        if ($this->type) {
            $request->setType($this->type);
        }

        if ($this->service) {
            $request->setService($this->service);
        }
        
        return $request;
    }
    
    public function getInsertRequest($id, $insertBind)
    {
        $request = $this->getRequest('add');
        return new StorageDataRequest(
            [$insertBind], 
            function ($insertBind) use ($request) {
                $request->setParams([$insertBind]);
                $request->send();
                return [$request];
            },
            function (DataRequest $request) {
                return $request->fetch();
            }
        );
    }
    
    /**
     * Метод мульти вставки данных.
     * Пока что эмулирует пакетно множество вставок паралелльно
     * 
     * @param $insertBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchInsertRequest($insertBindsByKeys)
    {
        $requests = [];
        foreach ($insertBindsByKeys as $id => $bind) {
            $requests[$id] = $this->getInsertRequest($id, $bind);       
        }
        
        return new StorageDataRequest(
            [$requests],
            function ($requests) {
                /* @var $requests StorageDataRequest[] */ 
                foreach ($requests as $id => $request) {
                    $request->send();
                }
                return [$requests];
            }, 
            function ($requests) {
                $results = [];
                
                /* @var $requests StorageDataRequest[] */
                foreach ($requests as $id => $request) {
                    $results[$id] = $request->fetch();
                }
                
                return $results;
            }
        );
    }
    
    public function getUpdateRequest($id, $bind)
    {
        $request = $this->getRequest('update');
        return new StorageDataRequest(
            [$id, $bind],
            function ($id, $bind) use ($request) {
                $request->setParams([$id, $bind]);
                $request->send();
                return [$request];
            },
            function (DataRequest $request) {
                return $request->fetch();
            }
        );
    }
    
    
    /**
     * @param $updateBindsByKeys
     *
     * @return StorageDataRequest
     * @throws \Exception
     */
    public function getBatchUpdateRequest($updateBindsByKeys)
    {
        throw new \Exception('NIY');
    }
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest
     */
    public function getReadRequest($ids)
    {
        $searchRequest = $this->getSearchRequest([$this->primaryKey => $ids], count($ids));
        
        return new StorageDataRequest(
            [$searchRequest, $this->primaryKey],
            function (StorageDataRequest $searchRequest, $primaryKey) {
                $searchRequest->send();
                
                return [$searchRequest, $primaryKey];
            },
            function (StorageDataRequest $searchRequest, $primaryKey) {
                if ($searchRequest->fetch()) {
                    return array_column($searchRequest->getResult(), null, $primaryKey);
                }
                
                return $searchRequest->getResult();
            }
        );
    }
    
    public function getSearchRequest($filter, $limit = 1, $conditions = [])
    {
        $request = $this->getRequest('search');
    
        $orders = [];
        if (isset($conditions['order'])) {
            $orders = $conditions['order'];
            unset($conditions['order']);
        }
        
        return new StorageDataRequest(
            [$filter, $conditions, $orders, $limit],
            function () use ($request) {
                $request->setParams(func_get_args());
                $request->send();
                return [$request];
            },
            function (DataRequest $request) {
                return $request->fetch();
            }
        );
    }
    
    public function getDeleteRequest ($ids) 
    {
        $request = $this->getRequest('delete');
    
        return new StorageDataRequest(
            [$ids],
            function ($ids) use ($request) {
                $request->setParams([$ids]);
                $request->send();
                return [$request];
            },
            function (DataRequest $request) {
                return $request->fetch();
            }
        );
    }
    
    /**
     * @return mixed
     */
    public function getModule()
    {
        return $this->module;
    }
    
    /**
     * @param mixed $module
     */
    public function setModule($module)
    {
        $this->module = $module;
    }
    
    /**
     * @return mixed
     */
    public function getController()
    {
        return $this->controller;
    }
    
    /**
     * @param mixed $controller
     */
    public function setController($controller)
    {
        $this->controller = $controller;
    }
    
    /**
     * @return mixed
     */
    public function getModel()
    {
        return $this->model;
    }
    
    /**
     * @param mixed $model
     */
    public function setModel($model)
    {
        $this->model = $model;
    }
    
    /**
     * @return mixed
     */
    public function getTimeout()
    {
        return $this->timeout;
    }
    
    /**
     * @param mixed $timeout
     */
    public function setTimeout($timeout)
    {
        $this->timeout = $timeout;
    }
    
    /**
     * @return mixed
     */
    public function getType()
    {
        return $this->type;
    }
    
    /**
     * @param mixed $type
     */
    public function setType($type)
    {
        $this->type = $type;
    }
    
    /**
     * @return mixed
     */
    public function getService()
    {
        return $this->service;
    }
    
    /**
     * @param mixed $service
     */
    public function setService($service)
    {
        $this->service = $service;
    }
}