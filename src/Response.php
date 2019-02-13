<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\ByteStream\PendingReadError;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Promise;
use Amp\Success;

class Response
{
    /** @var \Amp\Artax\Response */
    private $response;
    private $valuesBuffer = [];
    private $incompleteLineBuffer = '';
    /** @var \Amp\Deferred|null */
    private $pendingRead;
    /** @var \Amp\Coroutine */
    private $coroutine;
    private $complete = false;
    /** @var string */
    private $delimiter;

    /**
     * Response constructor.
     * @param \Amp\Artax\Response $response
     */
    public function __construct(\Amp\Artax\Response $response)
    {
        $this->response = $response;
    }

    /**
     * @return \Amp\Artax\Response
     */
    public function getHttpResponse(): \Amp\Artax\Response
    {
        return $this->response;
    }

    /**
     * @param string $delimiter
     * @return Promise
     */
    public function getValues(string $delimiter = "\t"): Promise
    {
        if ($this->pendingRead) {
            throw new PendingReadError;
        }

        if ($this->coroutine === null) {
            $this->delimiter = $delimiter;
            $this->coroutine = new Coroutine($this->consume());
        }

        if ($this->complete) {
            return new Success;
        }

        $this->pendingRead = new Deferred;
        return $this->pendingRead->promise();
    }

    private function consume(): \Generator
    {
        $this->valuesBuffer = $this->splitValues(yield $this->response->getBody()->read());
        while (!empty($this->valuesBuffer)) {
            $deferred = $this->pendingRead;
            $this->pendingRead = null;
            $deferred->resolve(array_shift($this->valuesBuffer));
            if (empty($this->valuesBuffer)) {
                $this->valuesBuffer = $this->splitValues(yield $this->response->getBody()->read());
            }
        }

        $this->complete = true;

        if ($this->pendingRead) {
            $deferred = $this->pendingRead;
            $this->pendingRead = null;
            $deferred->resolve();
            $this->valuesBuffer = null;
        }
    }

    private function splitValues(string $data = null): array
    {
        if (is_null($data)) {
            return [];
        }
        $lines = explode("\n", $data);
        $lines[0] = "{$this->incompleteLineBuffer}{$lines[0]}"; // Concat incomplete lines
        $linesCount = count($lines);
        $this->incompleteLineBuffer = $lines[$linesCount - 1]; // Save incomplete line
        unset($lines[$linesCount - 1]);
        return array_map(function ($line) {
            return str_getcsv($line, $this->delimiter);
        }, $lines);
    }
}
