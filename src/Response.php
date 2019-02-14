<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\ByteStream\Payload;
use Amp\Iterator;
use CSV\Options;
use CSV\Async\TsvParser;

class Response
{
    /** @var \Amp\Artax\Response */
    private $response;

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
     * @return Payload
     */
    public function getPayload(): Payload
    {
        return new Payload($this->response->getBody());
    }

    /**
     * @return Iterator
     */
    public function iterate(): Iterator
    {
        return (new TsvParser(Options::tsv()))->parse($this->getPayload());
    }
}
