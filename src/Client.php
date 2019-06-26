<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\Artax\DefaultClient as HttpClient;
use Amp\Artax\Request;
use Amp\Artax\RequestBody;
use Amp\ByteStream\InputStream;
use function Amp\call;
use Amp\Promise;
use Amp\Success;

class Client
{
    private $host;
    private $port;
    private $scheme = 'http';
    private $username;
    private $password;
    private $isCredentialsSet = false;
    /** @var HttpClient */
    private $httpClient;

    /**
     * Client constructor.
     * @param string $host
     * @param int $port
     * @param bool $useHttps
     */
    public function __construct(string $host = '127.0.0.1', int $port = 8123, bool $useHttps = false)
    {
        $this->host = $host;
        $this->port = $port;
        if ($useHttps) {
            $this->scheme = 'https';
        }
        $this->httpClient = new HttpClient();
        $this->httpClient->setOption(HttpClient::OP_MAX_BODY_BYTES, 0); // TODO: Add 'setOptions' method
        $this->httpClient->setOption(HttpClient::OP_TRANSFER_TIMEOUT, 0);
    }

    public function withCredentials(string $username, string $password): Client
    {
        $this->username = $username;
        $this->password = $password;
        $this->isCredentialsSet = true;
        return $this;
    }

    /**
     * @param string $sql
     * @param InputStream|null $data
     * @return Promise
     * @throws ClientException
     */
    public function query(string $sql, InputStream $data = null): Promise
    {
        return call(function () use ($sql, $data) {
            $request = new Request($this->makeUrl($sql), 'POST');

            if ($data) {
                $request = $request->withBody($this->makeBody($data));
            }

            /** @var \Amp\Artax\Response $httpResponse */
            $httpResponse = yield $this->httpClient->request($request);
            if ($httpResponse->getStatus() >= 400) {
                throw new ClientException(yield $httpResponse->getBody());
            }

            return new Response($httpResponse);
        });
    }

    /**
     * @param string $table
     * @param array|\Traversable $data
     * @return Promise
     * @throws ClientException
     */
    public function insert(string $table, $data): Promise
    {
        return $this->query(sprintf('INSERT INTO %s FORMAT TSV', $table), new TsvDataStream($data));
    }

    private function makeUrl(string $sql): string
    {
        $data = ['query' => $sql];
        if ($this->isCredentialsSet) {
            $data['user'] = $this->username;
            $data['password'] = $this->password;
        }
        return sprintf(
            '%s://%s:%d?%s',
            $this->scheme,
            $this->host,
            $this->port,
            http_build_query($data, '', '&', PHP_QUERY_RFC3986)
        );
    }

    private function makeBody(InputStream $data): RequestBody
    {
        return new class($data) implements RequestBody {

            /** @var InputStream */
            private $data;

            public function __construct(InputStream $data)
            {
                $this->data = $data;
            }

            public function getHeaders(): Promise
            {
                return new Success([]);
            }

            public function createBodyStream(): InputStream
            {
                return $this->data;
            }

            public function getBodyLength(): Promise
            {
                return new Success(-1);
            }
        };
    }
}
