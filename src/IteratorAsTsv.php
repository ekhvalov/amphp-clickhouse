<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\ByteStream\InputStream;
use Amp\ByteStream\PendingReadError;
use function Amp\call;
use Amp\Iterator;
use Amp\Promise;

class IteratorAsTsv implements InputStream
{
    /** @var Iterator */
    private $iterator;
    private $isMultiDimensional;
    private $stringEscapeCharList;

    /**
     * IteratorAsTsv constructor.
     * @param \Traversable|array $iterator
     * @param bool $isMultiDimensional
     * @param string $stringEscapeCharList
     */
    public function __construct($iterator, bool $isMultiDimensional = false, string $stringEscapeCharList = "\t\n\\")
    {
        $this->iterator = Iterator\fromIterable($iterator);
        $this->isMultiDimensional = $isMultiDimensional;
        $this->stringEscapeCharList = $stringEscapeCharList;
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
            while (yield $this->iterator->advance()) {
                $data = $this->iterator->getCurrent();
                if ($this->isMultiDimensional) {
                    return sprintf("%s\n", implode("\t", array_map(function ($value) {
                        return $this->convertValue($value);
                    }, $data)));
                }
                return sprintf("%s\n", $this->convertValue($data));
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

    private function convertObject(\StdClass $value)
    {
        if (method_exists($value, '__toString')) {
            return $this->convertString($value);
        }
        throw new ConvertException(sprintf("Object of class '%s' could not be converted to string", get_class($value)));
    }
}
