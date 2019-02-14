# amphp-clickhouse

## Create table
```php
\Amp\Loop::run(function () {
    $client = new Client();
    
    $sql = "create table t (date Date, array Array(UInt8), data Nullable(String), bool UInt8) ENGINE = Memory";
    
    yield $client->query($sql);
});
```

## Insert
```php
\Amp\Loop::run(function () {
    $client = new Client();
    
    $values = [
        ['1970-01-01', [1, 2], "Hello\tworld\n", false],
        ['1980-07-19', [203, 21], "foo\\bar", true],
    ];
    
    yield $client->insert('t', $values);
});
```
## Select

```php
\Amp\Loop::run(function () {
    $client = new Client();
    
    $sql = 'select * from table t';
    
    $response = yield $client->query($sql);
    $iterator = $response->iterate();
    
    while (yield $iterator->advance()) {
        $values = $iterator->getCurrent();
        // Do something with $values
    }
});
```
