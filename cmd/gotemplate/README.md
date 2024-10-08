CLI to solve https://github.com/tednaleid/ganda/issues/10

I threw together a quick golang CLI tool that could be used for generic values like this called `gotemplate`, it can use all of the various `printf` style values:

```
cat << EOF | ./gotemplate -d '{"string": "%s", "int": %5d, "left_int": %-5d, "zero_pad": %05d, "float": %8.2f, "left_float": %-8.2f, "generic": %v}'
hello 42 99 7 3.14159 2.718 world
goodnight 1234 5 123 0.1 100.5 3.1415
EOF
{"string": "hello", "int":    42, "left_int": 99   , "zero_pad": 00007, "float":     3.14, "left_float": 2.72    , "generic": world}
{"string": "goodnight", "int":  1234, "left_int": 5    , "zero_pad": 00123, "float":     0.10, "left_float": 100.50  , "generic": 3.1415}
```

This could be used to generate values that could be passed to `ganda`, but would also be generically useful for templating stdin and "form filling" values:

```
cat << EOF | ./gotemplate -d '{"url": "%s", "body": {"first": "%s", "second": %d}}'
http://httpbin.org/anything/1 bar 111
http://httpbin.org/anything/2 baz 222
http://httpbin.org/anything/3 qux 333
EOF
{"url": "http://httpbin.org/anything/1", "body": {"first": "bar", "second": 111}}
{"url": "http://httpbin.org/anything/2", "body": {"first": "baz", "second": 222}}
{"url": "http://httpbin.org/anything/3", "body": {"first": "qux", "second": 333}}
```