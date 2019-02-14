<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\Artax\DefaultClient;
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
            $httpResponse = yield $this->getHttpClient()->request($request);
            if ($httpResponse->getStatus() >= 400) {
                throw new ClientException(yield $httpResponse->getBody());
            }

            return new Response($httpResponse);
        });
    }

    private function getHttpClient(): \Amp\Artax\Client
    {
        $client = new DefaultClient();
        $client->setOption(DefaultClient::OP_MAX_BODY_BYTES, 0); // TODO: Add 'setOptions' method
        $client->setOption(DefaultClient::OP_TRANSFER_TIMEOUT, 0);
        return $client;
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
