# b2pipe

An easy way to stream an unknown amount of data into a file on b2 cloud.

# options

```
Options:
  --help         Show help                                             [boolean]
  --version      Show version number                                   [boolean]
  --concurrency  max concurrent connections                        [default: 20]
  --path         path on b2 container                                 [required]
  --bucket       bucket on b2                                         [required]
  --type         content-type for the file being uploaded       [default: "raw"]
  --account      b2 account                                           [required]
  --key          b2 key                                               [required]
  --silent       will not output to stderr                      [default: false]
  --attempts     max upload attempts                               [default: 30]
  --chunk                                                     [default: 5000000]
```

# example

Extremely simple example

```
echo "hello world" | b2pipe --account B2_ACCOUNT --key B2_KEY --bucket B2_BUCKET --path "helloworld.txt" --type "text/plain"
```

Of course you can do more complex things like the following

```
zfs send pool@latest | pigz | b2pipe --concurrency 20 --account B2_ACCOUNT --key B2_KEY --bucket B2_BUCKET --path "pool.gz"
```

Which would effectively stream+gzip, a ZFS pool to b2 cloud.
