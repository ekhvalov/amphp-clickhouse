# amphp-clickhouse

## Usage

```php
$client = new Client();
$sql = 'select * from table';
$response = yield $client->query($sql);
while (($values = yield $response->getValues()) !== null) {
    // Do something ...
}
```