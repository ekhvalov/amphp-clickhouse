<?php

namespace Ekhvalov\AmphpClickHouse
{
    /**
     * @param array $values [val_a, val_b, ...]
     * @return string
     */
    function valuesToString(array $values): string
    {
        return sprintf('(%s)', implode(', ', \array_map(function ($value) {
            return \is_string($value) ? "'{$value}'" : $value;
        }, $values)));
    }

    /**
     * @param array $valuesRows [[val_a1, val_b1], [val_a2, val_b2], ...]
     * @return string
     */
    function valuesRowsToString(array $valuesRows): string
    {
        return implode(', ', array_map(function ($values) {
            return valuesToString($values);
        }, $valuesRows));
    }
}
