<?php


namespace Verse\Storage\Data;

use Verse\Storage\Request\StorageDataRequest;
use Verse\Storage\Spec\Compare;

class JBaseDataAdapter extends DataAdapterProto
{
    private const READ_ACCESS   = 'r';
    private const CREATE_ACCESS = 'w';
    private const UPDATE_ACCESS = 'c+';
    private const ADD_ACCESS    = 'x';
    
    const F_DATA = 'd';
    const F_TOUCHED = 't';
    
    private $dataRoot = '/tmp/jbase';
    
    private $database = 'default';
    
    private $_dirCheckCache = [];
    
    static private $sortingRules = [
        'asc'  => SORT_ASC,
        'desc' => SORT_DESC,
    ];
    
    private $_searchFilters = [];
    
    private function _getTablePath() {
        $di = DIRECTORY_SEPARATOR;
        $path = $this->dataRoot.$di.$this->database.$di.$this->resource.$di;
        if (isset($this->_dirCheckCache[$path])) {
            return $path;
        }
        
        if (!file_exists($path)) {
            if (!mkdir($path, 0774, true) && !is_dir($path)) {
                throw new \RuntimeException(sprintf('Table path "%s" was not created', $path));
            }
            chmod($path, 0774);
        }
        
        $this->_dirCheckCache[$path] = file_exists($path);
        
        return $path;
    }
    
    public function getPointer($id, $method = self::READ_ACCESS) {
        if(!$id) {
            return null;
        }

        $filePath = $this->_getTablePath().$id;
        $fileExists = file_exists($filePath);
        if ($method === self::READ_ACCESS && !$fileExists) {
            return null;
        }
        
        if ($method === self::ADD_ACCESS && $fileExists) {
            return null;
        }
            
        return fopen($filePath, $method);    
    }
    
    public function closePointer ($resource) 
    {
        return fclose($resource);
    }
    
    public function getAllItems() {
        $list = scandir($this->_getTablePath(), SCANDIR_SORT_NONE);
        return array_diff($list, ['.','..']);
    }
    
    /**
     * @param $id         null|int|array
     * @param $insertBind array
     *
     * @return StorageDataRequest
     */
    public function getInsertRequest($id, $insertBind)
    {
        $self = $this;
        $request = new StorageDataRequest(
            [$id, $insertBind],
            function ($id, $bind) use ($self) {
                $pointer = $self->getPointer($id, self::ADD_ACCESS);
                if (!$pointer) {
                    return null;
                }
                $res = fwrite($pointer, $this->_packData($bind));
                $self->closePointer($pointer);
                return $res ? [$this->primaryKey => $id] + $bind : null;
            }
        );
        
        return $request;
    }
    
    private function _packData($data) {
        $bind = [
            self::F_DATA => is_array($data) ? $data : [$data],
            self::F_TOUCHED => microtime(1)
        ];
        
        return \json_encode($bind, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT); 
    }
    
    /**
     * @param $insertBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchInsertRequest($insertBindsByKeys) : StorageDataRequest
    {
        $self = $this;
        $request = new StorageDataRequest(
            [$insertBindsByKeys],
            function ($insertBindsByKeys) use ($self) {
                $results = [];
                foreach ($insertBindsByKeys as $id => $bind) {
                    $pointer = $self->getPointer($id, self::ADD_ACCESS);
                    if (!$pointer) {
                        $results[$id] = false;
                    }
                    
                    $res = fwrite($pointer, $this->_packData($bind));
                    $self->closePointer($pointer);

                    $results[$id] = $res ? true : false; 
                }
                
                return $results;
            }
        );

        return $request;
    }
    
    /**
     * @param $id int|array
     * @param $updateBind
     *
     * @return StorageDataRequest
     */
    public function getUpdateRequest($id, $updateBind)
    {
        $self = $this;
        $updateBind = [$this->primaryKey => $id] + $updateBind;
        $request = new StorageDataRequest(
            [$id, $updateBind],
            function ($id, $bind) use ($self) {
                $pointer = $self->getPointer($id, self::UPDATE_ACCESS);
                if (!$pointer) {
                    return null;
                }
    
                $record = json_decode(stream_get_contents($pointer), true);
                if (isset($record[self::F_DATA])) {
                    $bind += $record[self::F_DATA];
                }
                ftruncate($pointer, 0);
                rewind($pointer);
                $res = fwrite($pointer, $this->_packData($bind));
                $self->closePointer($pointer);
                return $res ? $bind : null;
            }
        );
    
        return $request;
    }
    
    /**
     * @param $updateBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchUpdateRequest($updateBindsByKeys)
    {
        $self = $this;
        $request = new StorageDataRequest(
            [$updateBindsByKeys],
            function ($updateBindsByKeys) use ($self) {
                $results = [];
                
                foreach ($updateBindsByKeys as $id => $bind) {
                    $pointer = $self->getPointer($id, self::UPDATE_ACCESS);
                    if (!$pointer) {
                        $results[$id] = false;
                    }

                    $record = json_decode(stream_get_contents($pointer), true);
                    
                    if (isset($record[self::F_DATA])) {
                        $bind += $record[self::F_DATA];
                    }
                    
                    ftruncate($pointer, 0);
                    rewind($pointer);
                    $res = fwrite($pointer, $this->_packData($bind));
                    $self->closePointer($pointer);
                    
                    $results[$id] = $res ? true : false;
                }

                return $results;
            }
        );

        return $request;
    }
    
    /**
     * @param $ids
     *
     * @return StorageDataRequest fetch result is array [ 'key1' => ['primary' => 'key1', ...], ... ]
     */
    public function getReadRequest($ids)
    {
        $self = $this;
        $request = new StorageDataRequest(
            [$ids],
            function ($ids) use ($self) {
                $results = [];
                foreach ($ids as $id) {
                    $pointer = $self->getPointer($id);
                    if ($pointer) {
                        $record = json_decode(stream_get_contents($pointer), true);
                        if (isset($record[self::F_DATA])) {
                            $results[$id] = $record[self::F_DATA] + [$this->primaryKey => $id];
                        }
                        $self->closePointer($pointer);
                    } else {
                        $results[$id] = null;
                    }
                }
                
                return $results;
            }
        );
    
        return $request;
    }
    
    /**
     * @param $ids int|array
     *
     * @return StorageDataRequest
     */
    public function getDeleteRequest($ids)
    {
        $self = $this;
        $request = new StorageDataRequest(
            [$ids],
            function ($ids) use ($self) {
                $tablePath = $self->_getTablePath();
                $results = [];
                foreach ($ids as $id) {
                    $filePath = $tablePath.$id;
                    $results[$id] = unlink($filePath);
                }
            
                return $results;
            }
        );
        
        return $request;
    }
    
    /**
     * @param array $filter
     *
     * @param int   $limit
     * @param array $conditions
     *
     * @return StorageDataRequest
     */
    public function getSearchRequest($filter, $limit = 1, $conditions = [])
    {
        $self = $this;
        $knownFilters = $this->_getSearchFilters();
        $request = new StorageDataRequest(
            [$filter, $limit, $conditions],
            function ($filter, $limit, $conditions) use ($self, $knownFilters) {
                $items = $self->getAllItems();
                $results = [];
                $filterRules = [];
                
                foreach ($filter as $filterRequest) {
                    if (\count($filterRequest) === 3) {
                        list ($key, $compare, $compareValue) = $filterRequest;
                        
                        if (isset($knownFilters[$compare])) {
                            $filterRules[] = [$key, $knownFilters[$compare], $compareValue];
                        } else {
                            trigger_error('Comparator not found for filter: '.json_encode($filterRequest).'');
                        }
                    } else {
                        trigger_error('Strange filter: less than 3 parameters, '.json_encode($filterRequest));
                    }
                }
                
                foreach ($items as $id) {
                    $pointer = $self->getPointer($id);
                    $isOk = true;
                    if ($pointer) {
                        $record = json_decode(stream_get_contents($pointer), true);
                        
                        if (isset($record[self::F_DATA])) {
                            foreach ($filterRules as $filterRule) {
                                list ($key, $function, $filterRequest) = $filterRule;
        
                                $isOk = $isOk && call_user_func($function, $record[self::F_DATA][$key] ?? null, $filterRequest);
        
                                if (!$isOk) {
                                    break;
                                }
                            }
    
                            if ($isOk) {
                                $results[$id] = [$this->primaryKey => $id] + $record[self::F_DATA];
                            }    
                        }
                        
                        $self->closePointer($pointer);
                        
                        if (count($results) >= $limit) {
                            break;
                        }
                    }
                }
                
                if ($results && $conditions) {
                    $self->applyConditions($results, $conditions);
                }
            
                return $results;
            }
        );
    
        return $request;
    }
    
    
    private function _getSearchFilters() {
        if (!$this->_searchFilters) {
            $this->_searchFilters = [
                Compare::EQ              => function ($original, $compare) {
                    return $original === $compare;
                },
                Compare::NOT_EQ          => function ($original, $compare) {
                    return $original !== $compare;
                },
                Compare::EMPTY_OR_EQ     => function ($original, $compare) {
                    return $original === null || $original === $compare;
                },
                Compare::EMPTY_OR_NOT_EQ => function ($original, $compare) {
                    return $original === null || $original !== $compare;
                },
                Compare::GRATER          => function ($original, $compare) {
                    return $original > $compare;
                },
                Compare::LESS            => function ($original, $compare) {
                    return $original > $compare;
                },
                Compare::GRATER_OR_EQ    => function ($original, $compare) {
                    return $original >= $compare;
                },
                Compare::LESS_OR_EQ      => function ($original, $compare) {
                    return $original <= $compare;
                },
                Compare::IN              => function ($original, $compare) {
                    return \in_array($original, $compare, true);
                },
                Compare::ANY             => function ($original, $compare) {
                    return \in_array($compare, $original, true);
                },
                Compare::STR_BEGINS      => function ($original, $compare) {
                    return stripos($original, $compare) === 0;
                },
                Compare::STR_ENDS        => function ($original, $compare) {
                    $length = strlen($compare);
                    return $length === 0 || (substr($original, -$length) === $compare);
                }
            ];
        }
        
        return $this->_searchFilters;
    }
    
    public function applyConditions (&$results, $conditions) 
    {
        if ($conditions['sort']) {
            $rows = [];
            
            foreach ($conditions['sort'] as $sortingRule) {
                $rule = null;
                $key = array_shift($sortingRule);
                if($sortingRule) {
                    $rule = array_shift($sortingRule);
                }
        
                $rows[$key] = self::$sortingRules[$rule] ?? SORT_ASC;
            }
            
            $params = [];
            foreach ($rows as $rowName => $rule) {
                $params[] = array_column($results, $rowName);
                $params[] = $rule;
            }
    
            $params[] = &$results;
            
            array_multisort(...$params);
        }
    }
    
    /**
     * @return string
     */
    public function getDataRoot(): string
    {
        return $this->dataRoot;
    }
    
    /**
     * @param string $dataRoot
     */
    public function setDataRoot(string $dataRoot)
    {
        $this->dataRoot = $dataRoot;
    }
    
    /**
     * @return string
     */
    public function getDatabase(): string
    {
        return $this->database;
    }
    
    /**
     * @param string $database
     */
    public function setDatabase(string $database)
    {
        $this->database = $database;
    }
    
}