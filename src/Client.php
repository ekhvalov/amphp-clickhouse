<?php

namespace Ekhvalov\AmphpClickHouse;

use Amp\Artax\DefaultClient;
use Amp\Artax\Request;
use Amp\Artax\RequestBody;
use function Amp\call;
use Amp\Promise;

class Client
{
    private $host;
    private $port;
    private $username;
    private $password;
    private $isCredentialsSet = false;

    /**
     * Client constructor.
     * @param string $host
     * @param int $port
     */
    public function __construct(string $host, int $port = 8123)
    {
        $this->host = $host;
        $this->port = $port;
    }

    public function withCredentials(string $username, string $password): Client
    {
        $this->username = $username;
        $this->password = $password;
        $this->isCredentialsSet = true;
        return $this;
    }

    public function query(string $sql, RequestBody $body = null): Promise
    {
        return call(function () use ($sql, $body) {
            $request = new Request($this->makeUrl($sql), 'POST');

            if ($body) {
                $request = $request->withBody($body);
            }

            return new Response(yield $this->getHttpClient()->request($request));
        });
    }

    private function getHttpClient(): \Amp\Artax\Client
    {
        $client = new DefaultClient();
        $client->setOption(DefaultClient::OP_MAX_BODY_BYTES, 0); // TODO: Add 'setOptions' method
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
            'http://%s:%d?%s', // TODO: Add scheme
            $this->host,
            $this->port,
            http_build_query($data, '', '&', PHP_QUERY_RFC3986)
        );
    }
}
