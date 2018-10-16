<?php


namespace Verse\Storage\Data;

use PDO;
use Psr\Log\LoggerInterface;
use Verse\Di\Env;
use Verse\Storage\Request\StorageDataRequest;
use Verse\Storage\Spec\Compare;

class PostgresTableDataAdapter extends DataAdapterProto
{
    private const CONNECTION_STATE_OK = 'Connection OK';
    /**
     * @var \PDO
     */
    protected $_connection;

    protected $table = '';

    /**
     * @var LoggerInterface
     */
    protected $logger = false;

    /**
     * @var array
     */
    protected $primaryKeyArray;

    protected $constraintHints = [];

    public function logError($error, $context = [])
    {
        if ($this->logger === false) {
            $this->logger = Env::getContainer()->bootstrap(LoggerInterface::class, false);
        }

        if ($this->logger) {
            $this->logger->error($error, $context);
        }
    }

    private function _getPrimaryKetArray() : array
    {
        if ($this->primaryKeyArray !== null) {
            return $this->primaryKeyArray;
        }

        if ($this->primaryKey === null || $this->primaryKey === '') {
            return $this->primaryKeyArray = [];
        }

        if (strpos($this->primaryKey, ',') !== false) {
            $this->primaryKeyArray = \explode(',', $this->primaryKey);
            array_walk($this->primaryKeyArray, function (&$key) {
                $key = trim($key);
            });
            return $this->primaryKeyArray;
        }

        return $this->primaryKeyArray = [$this->primaryKey];
    }

    /**
     * @return \PDO|null
     */
    private function _getConnection()
    {
        if ($this->_connection) {
            if (strpos($this->_connection->getAttribute(PDO::ATTR_CONNECTION_STATUS), self::CONNECTION_STATE_OK) !== false) {
                return $this->_connection;    
            }
        }

        try {
            $this->_connection = new PDO($this->resource, null, null, [
                PDO::ATTR_PERSISTENT => true 
            ]);
        } catch (\PDOException $e) {
            $this->logError('Cannot connect to database: ' . __METHOD__);
        }

        return $this->_connection;
    }

    /**
     * @param $id null|int|array
     * @param $insertBind array
     *
     * @return StorageDataRequest
     */
    public function getInsertRequest($id, $insertBind)
    {
        $db = $this->_getConnection();
        $table = $this->table;
        $self = $this;
        $primary = $this->_getPrimaryKetArray();

        $request = new StorageDataRequest(
            [$id, $insertBind],
            function ($id, $bind) use ($db, $table, $self, $primary) {
                if (!$db) {
                    $self->logError('Missing connection on getInsertRequest on table: ' . $table);
                    return false;
                }

                $keyBind = $self->prepareKey($primary, $id);

                $keys = [];
                $values = [];
                $bind = $self->prepareBind($keyBind + $bind, $db);

                foreach ($bind as $key => $value) {
                    $keys[] = $key;
                    $values[] = $value;
                }

                $query = 'INSERT INTO ' . $table . ' as t '
                    . ' (' . implode(',', $keys) . ') VALUES (' . implode(',', $values) . ')'
                    . $self->makeConstraintHintPostfix()
                    . ' RETURNING *';

                $stmnt = $db->query($query);

                if (!$stmnt) {
                    $this->logError('Cannot execute getInsertRequest on table: ' . $table . ':' . implode(',', $db->errorInfo()), [
                        'code' => $db->errorCode(),
                    ]);

                    return false;
                }

                $result = $stmnt->fetch(PDO::FETCH_ASSOC);

                if (!$result && (int) $stmnt->errorCode() > 0) {
                    $this->logError('Error on execution on getInsertRequest on table: ' . $table . ':' . implode(',',
                            $stmnt->errorInfo()), [
                        'code' => $stmnt->errorCode(),
                    ]);

                    return false;
                }


                return $result;
            });

        return $request;
    }

    /**
     * @param $insertBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchInsertRequest($insertBindsByKeys)
    {
        $db = $this->_getConnection();
        $table = $this->table;
        $self = $this;
        $primary = $this->_getPrimaryKetArray();

        $request = new StorageDataRequest(
            [$insertBindsByKeys],
            function ($insertBindsByKeys) use ($db, $table, $self, $primary) {
                if (!$db) {
                    $self->logError('Missing connection on getBatchInsertRequest on table: ' . $table);
                    return false;
                }

                $firstElement = \reset($insertBindsByKeys);
                if (!$firstElement) {
                    $self->logError('Empty bind or first element on getBatchInsertRequest on table: ' . $table);
                    return [];
                }

                $keys = array_unique(array_merge($primary, array_keys($firstElement)));

                $primaryHintApplicable = \count($primary) === 1;

                $valuesArray = [];
                foreach ($insertBindsByKeys as $id => $bind) {
                    $values = [];

                    if ($primaryHintApplicable) {
                        $bind = $self->prepareKey($primary, $id) + $bind;
                    }

                    $bind = $self->prepareBind($bind, $db);
                    foreach ($keys as $key) {
                        $values[] = $bind[$key] ?? null;
                    }

                    $valuesArray[] = '(' . implode(',', $values) . ')';
                }

                $query = 'INSERT INTO ' . $table . ' as t '
                    . ' (' . implode(',', $keys) . ') VALUES '
                    . implode(',', $valuesArray)
                    . $self->makeConstraintHintPostfix()
                    . ' RETURNING *';

                $stmnt = $db->query($query);

                if (!$stmnt) {
                    $self->logError('Cannot execute getBatchInsertRequest on table: ' . $table . ':' . implode(',', $db->errorInfo()), [
                        'code' => $db->errorCode(),
                    ]);

                    return false;
                }

                $result = $stmnt->fetchAll(PDO::FETCH_ASSOC);

                if (!$result && (int) $stmnt->errorCode() > 0) {
                    $self->logError('Error on execution on getBatchInsertRequest on table: ' . $table . ':' . implode(',',
                            $stmnt->errorInfo()), [
                        'code' => $stmnt->errorCode(),
                    ]);

                    return false;
                }

                return $result;
            });

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
        $db = $this->_getConnection();
        $table = $this->table;
        $self = $this;
        $primary = $this->_getPrimaryKetArray();

        $request = new StorageDataRequest(
            [$id, $updateBind],
            function ($id, $updateBind) use ($db, $table, $self, $primary) {
                if (!$db) {
                    $self->logError('Missing connection on getUpdateRequest on table: ' . $table);
                    return false;
                }

                $keyBind = $self->prepareKey($primary, $id);

                $whereBind = $self->prepareBind($keyBind, $db);
                $whereArray = [];
                foreach ($whereBind as $key => $value) {
                    $whereArray[] = $key . ' = ' . $value;
                }

                $keys = [];
                $values = [];

                $updateBind = $self->prepareBind($updateBind, $db);

                foreach ($updateBind as $key => $value) {
                    $keys[] = $key;
                    $values[] = $value;
                }

                $query = 'UPDATE ' . $table
                    . ' SET (' . implode(',', $keys) . ') = ROW(' . implode(',', $values) . ') '
                    . ' WHERE ' . implode(' AND ', $whereArray)
                    . ' RETURNING *';
                $stmnt = $db->query($query);

                if (!$stmnt) {
                    $this->logError('Cannot execute getUpdateRequest on table: ' . $table . ':' . implode(',', $db->errorInfo()), [
                        'code' => $db->errorCode(),
                    ]);

                    return false;
                }

                $result = $stmnt->fetch(PDO::FETCH_ASSOC);

                if (!$result && (int) $stmnt->errorCode() > 0) {
                    $this->logError('Error on execution on getUpdateRequest on table: ' . $table . ':' . implode(',',
                            $stmnt->errorInfo()), [
                        'code' => $stmnt->errorCode(),
                    ]);

                    return false;
                }

                return $result;
            });

        return $request;
    }

    /**
     * @param $updateBindsByKeys
     *
     * @return StorageDataRequest
     */
    public function getBatchUpdateRequest($updateBindsByKeys)
    {
        throw new \RangeException('NIY');
    }

    /**
     * @param $ids
     *
     * @return StorageDataRequest fetch result is array [ 'key1' => ['primary' => 'key1', ...], ... ]
     */
    public function getReadRequest($ids)
    {
        $db = $this->_getConnection();
        $table = $this->table;
        $self = $this;
        $primary = $this->_getPrimaryKetArray();

        $request = new StorageDataRequest(
            [$ids],
            function ($ids) use ($db, $table, $self, $primary) {
                if (!$db) {
                    $self->logError('Missing connection on getReadRequest on table: ' . $table);
                    return false;
                }

                $whereLines = [];
                foreach ($ids as $id) {
                    $whereArray = [];
                    $whereBind = $self->prepareBind($self->prepareKey($primary, $id), $db);
                    foreach ($whereBind as $key => $value) {
                        $whereArray[] = $key . ' = ' . $value;
                    }
                    $whereLines[] = implode(' AND ', $whereArray);
                }


                $query = 'SELECT * FROM ' . $table
                    . ' WHERE (' . implode(' ) OR (', $whereLines) . ')';


                $stmnt = $db->query($query);

                if (!$stmnt) {
                    $this->logError('Cannot execute getReadRequest on table: ' . $table . ':' . implode(',', $db->errorInfo()), [
                        'code' => $db->errorCode(),
                    ]);

                    return false;
                }

                $result = $stmnt->fetchAll(PDO::FETCH_ASSOC);

                if (!$result && (int) $stmnt->errorCode() > 0) {
                    $this->logError('Error on execution on getReadRequest on table: ' . $table . ':' . implode(',',
                            $stmnt->errorInfo()), [
                        'code' => $stmnt->errorCode(),
                    ]);

                    return false;
                }

                return $result;
            });

        return $request;
    }

    /**
     * @param $ids int|array
     *
     * @return StorageDataRequest
     */
    public function getDeleteRequest($ids)
    {
        $db = $this->_getConnection();
        $table = $this->table;
        $self = $this;
        $primary = $this->_getPrimaryKetArray();

        $request = new StorageDataRequest(
            [$ids],
            function ($ids) use ($db, $table, $self, $primary) {
                if (!$db) {
                    $self->logError('Missing connection on getDeleteRequest on table: ' . $table);
                    return false;
                }

                $whereLines = [];
                foreach ($ids as $id) {
                    $whereArray = [];
                    $whereBind = $self->prepareBind($self->prepareKey($primary, $id), $db);
                    foreach ($whereBind as $key => $value) {
                        $whereArray[] = $key . ' = ' . $value;
                    }
                    $whereLines[] = implode(' AND ', $whereArray);
                }

                $query = 'DELETE FROM ' . $table
                    . ' WHERE (' . implode(' ) OR (', $whereLines) . ')';

                $stmnt = $db->query($query);

                if (!$stmnt) {
                    $this->logError('Cannot execute getDeleteRequest on table: ' . $table . ':' . implode(',', $db->errorInfo()), [
                        'code' => $db->errorCode(),
                    ]);

                    return false;
                }

                $result = $stmnt->fetchAll(PDO::FETCH_ASSOC);

                if (!$result && (int) $stmnt->errorCode() > 0) {
                    $this->logError('Error on execution on getDeleteRequest on table: ' . $table . ':' . implode(',',
                            $stmnt->errorInfo()), [
                        'code' => $stmnt->errorCode(),
                    ]);

                    return false;
                }

                return $result;
            });

        return $request;
    }

    /**
     * @param array $filter
     *
     * @param int $limit
     * @param array $conditions
     *
     * @return StorageDataRequest
     */
    public function getSearchRequest($filter, $limit = 1, $conditions = [])
    {
        $db = $this->_getConnection();
        $table = $this->table;
        $self = $this;

        $request = new StorageDataRequest(
            [$filter, $limit],
            function ($filter, $limit) use ($db, $table, $self) {
                if (!$db) {
                    $self->logError('Missing connection on getSearchRequest on table: ' . $table);
                    return false;
                }
                
                //
                $filterBinds = [];
                foreach ($filter as $id => $filterItem) {
                    $filterBinds[$id] = $filterItem[2];  
                }
                //
                $filterBinds = $this->prepareBind($filterBinds, $db, false);
                //
                foreach ($filter as $id => &$filterItem) {
                    $filterItem[2] = $filterBinds[$id];
                } unset($filterItem);
                
                //
                $filtersWhereArray = $this->_prepapreFilters($filter);
                
                $query = 'SELECT * FROM ' . $table
                    . ' WHERE (' . implode(' ) AND (', $filtersWhereArray) . ')'
                    . ' LIMIT '.$limit;

                $stmnt = $db->query($query);

                if (!$stmnt) {
                    $this->logError('Cannot execute getSearchRequest on table: ' . $table . ':' . implode(',', $db->errorInfo()), [
                        'code' => $db->errorCode(),
                    ]);

                    return false;
                }

                $result = $stmnt->fetchAll(PDO::FETCH_ASSOC);

                if (!$result && (int) $stmnt->errorCode() > 0) {
                    $this->logError('Error on execution on getSearchRequest on table: ' . $table . ':' . implode(',',
                            $stmnt->errorInfo()), [
                        'code' => $stmnt->errorCode(),
                    ]);

                    return false;
                }

                return $result;
            });

        return $request;
    }

    /**
     * @return string
     */
    public function getTable() : string
    {
        return $this->table;
    }

    /**
     * @param string $table
     */
    public function setTable(string $table) : void
    {
        $this->table = $table;
    }

    const VAR_BOOLEAN      = 'boolean';
    const VAR_INTEGER      = 'integer';
    const VAR_DOUBLE       = 'double';
    const VAR_STRING       = 'string';
    const VAR_ARRAY        = 'array';
    const VAR_OBJECT       = 'object';
    const VAR_RESOURCE     = 'resource';
    const VAR_NULL         = 'NULL';
    const VAR_UNKNOWN_TYPE = 'unknown type';

    public function prepareKey($structure, $data)
    {
        $data = \is_array($data) ? $data : [$data];

        foreach ($data as &$key) {
            $key = $key ?? 'DEFAULT';
        }
        unset($key);

        return array_combine($structure, $data);
    }

    public function _prepapreFilters($filters)
    {
        $filterGenerators = [
            Compare::EQ              => function ($key, $value) {
                if ($value === null) {
                    return $key . ' IS NULL ';
                }
                return $key . ' = ' . $value;
            },
            Compare::NOT_EQ          => function ($key, $value) {
                if ($value === null) {
                    return $key . ' IS NOT NULL ';
                }
                return $key . ' != ' . $value;
            },
            Compare::EMPTY_OR_EQ     => function ($key, $value) {
                return $key . ' IS NULL OR ' . $key . ' = ' . $value;
            },
            Compare::EMPTY_OR_NOT_EQ => function ($key, $value) {
                return $key . ' IS NULL OR ' . $key . ' != ' . $value;
            },
            Compare::GRATER          => function ($key, $value) {
                return $key . ' > ' . $value;
            },
            Compare::LESS            => function ($key, $value) {
                return $key . ' < ' . $value;
            },
            Compare::GRATER_OR_EQ    => function ($key, $value) {
                return $key . ' >= ' . $value;
            },
            Compare::LESS_OR_EQ      => function ($key, $value) {
                return $key . ' <= ' . $value;
            },
            Compare::IN              => function ($key, $values) {
                if (empty($values)) {
                    return 'FALSE'; 
                }
                return $key . ' IN (' . implode(',', $values). ')';
            },
            Compare::ANY             => function ($key, $values) {
                if (empty($values)) {
                    return 'FALSE';
                }
                
                if (!\is_array($values)) {
                    $values = [$values];
                }
                
                return $key . '  @> ARRAY[' . implode(',', $values). ']';
            },
            Compare::STR_BEGINS      => function ($key, $value) {
                return $key .' LIKE '.\mb_substr($value, 0, -1).'%\'';
            },
            Compare::STR_ENDS        => function ($key, $value) {
                return $key .' LIKE \'%'.\mb_substr($value, 1);
            },
        ];

        $filterConditions = [];
        foreach ($filters as $filterRequest) {
            if (\count($filterRequest) === 3) {
                list ($key, $compare, $compareValue) = $filterRequest;

                if (isset($filterGenerators[$compare])) {
                    $function = $filterGenerators[$compare];
                    $filterConditions[] = $function($key, $compareValue);
                } else {
                    trigger_error('Comparator not found for filter: ' . json_encode($filterRequest) . '');
                }
            } else {
                trigger_error('Strange filter: less than 3 parameters, ' . json_encode($filterRequest));
            }
        }

        return $filterConditions;
    }

    public function prepareBind($bind, PDO $db, $jsonifyArrays = true)
    {
        foreach ($bind as &$value) {
            if ($value === 'DEFAULT') { // serial keys hint
                continue;
            }
 
            switch (gettype($value)) {
                case self::VAR_STRING;
                    $value = $db->quote($value);
                    break;
                case self::VAR_BOOLEAN;
                    $value = $value ? 'TRUE' : 'FALSE';
                    break;
                case self::VAR_NULL;
                case self::VAR_RESOURCE;
                case self::VAR_UNKNOWN_TYPE;
                    $value = 'NULL';
                    break;
                case self::VAR_INTEGER;
                case self::VAR_DOUBLE;
                    break;
                case self::VAR_ARRAY;
                    if ($jsonifyArrays) {
                        $value = $db->quote(json_encode($value, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE));
                    } else {
                        $value = $this->prepareBind($value, $db, $jsonifyArrays);
                    }
                    break;
                case self::VAR_OBJECT;
                    $value = $db->quote(json_encode($value, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE));
                    break;
            }
        }

        return $bind;
    }


    public function makeConstraintHintPostfix()
    {
        $postfix = '';
        foreach ($this->constraintHints as $constraint => $hint) {
            $postfix .= ' ON CONFLICT ON CONSTRAINT ' . $constraint . ' DO ' . $hint;
        }

        return $postfix;
    }

    public function clearTable()
    {
        $db = $this->_getConnection();
        return $db && $db->exec('TRUNCATE ' . $this->getTable());
    }

    public function executeQuery($query)
    {
        $db = $this->_getConnection();
        return $db && $db->exec($query);
    }

    /**
     * @return array
     */
    public function getConstraintHints() : array
    {
        return $this->constraintHints;
    }

    /**
     * @param array $constraintHints
     */
    public function setConstraintHints(array $constraintHints) : void
    {
        $this->constraintHints = $constraintHints;
    }
}