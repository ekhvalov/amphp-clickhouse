<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\ByteStream\InputStream;
use Amp\ByteStream\PendingReadError;
use function Amp\call;
use Amp\Iterator;
use Amp\Promise;

class TsvDataStream implements InputStream
{
    /** @var Iterator */
    private $rows;
    private $stringEscapeCharList;
    private $isJsonSupported;

    /**
     * TsvDataStream constructor.
     * @param array|\Traversable|Iterator $rows
     * @param string $stringEscapeCharList
     */
    public function __construct($rows, string $stringEscapeCharList = "\t\n\\")
    {
        $this->rows = ($rows instanceof Iterator) ? $rows : Iterator\fromIterable($rows);
        $this->stringEscapeCharList = $stringEscapeCharList;
        $this->isJsonSupported = extension_loaded('json');
    }

    /**
     * Reads data from the stream.
     *
     * @return Promise Resolves with a string when new data is available or `null` if the stream has closed.
     *
     * @throws PendingReadError Thrown if another read operation is still pending.
     * @throws \Error
     * @throws \Throwable
     */
    public function read(): Promise
    {
        return call(function () {
            while (yield $this->rows->advance()) {
                return sprintf("%s\n", implode("\t", array_map(function ($value) {
                    return $this->convertValue($value);
                }, $this->rows->getCurrent())));
            }
            return null;
        });
    }

    private function convertValue($value)
    {
        switch ($type = gettype($value)) {
            case 'boolean':
                return intval($value);
            case 'integer':
                return $value;
            case 'double':
                return $value;
            case 'string':
                return $this->convertString($value);
            case 'array':
                return $this->convertArray($value);
            case 'object':
                return $this->convertObject($value);
            case 'NULL':
                return $type;
            default:
                throw new ConvertException(sprintf("Value of type '%s' could not be converted to string", $type));
        }
    }

    private function convertString(string $value): string
    {
        return addcslashes($value, $this->stringEscapeCharList);
    }

    private function convertArray(array $values): string
    {
        return sprintf('[%s]', implode(',', array_map(function ($value) {
            return $this->convertValue($value);
        }, $values)));
    }

    private function convertObject($value)
    {
        if (method_exists($value, '__toString')) {
            return $this->convertString($value);
        } elseif ($this->isJsonSupported && ($value instanceof \JsonSerializable)) {
            return $this->convertString(\json_encode($value));
        }
        throw new ConvertException(sprintf("Object of class '%s' could not be converted to string", get_class($value)));
    }
}
