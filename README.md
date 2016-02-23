HTTP
======
[![Swift 2.2](https://img.shields.io/badge/Swift-2.2-orange.svg?style=flat)](https://swift.org)
[![License MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat)](https://tldrlegal.com/license/mit-license)

A pure Swift client for Redis.

- [x] TCP
- [ ] hiredis

## Installing

First install [libvenice](https://github.com/Zewo/libvenice):

```bash
brew tap zewo/tap
brew install libvenice
```

Then add `Swift-Redis` to your `Package.swift`

```swift
import PackageDescription

let package = Package(
    dependencies: [
        .Package(url: "https://github.com/rabc/Swift-Redis", majorVersion: 0, minor: 1)
    ]
)
```

## Using

```swift
let redis = try Redis("172.28.128.3", 6379)
try redis.command(.SET("foo", "bar"))
```
All commands and its parameters are defined in `CommandTypeEnum` enum. The `command` function returns the same response from Redis.

## Contributing

Pull requests are welcome.

Now, all commands are being sent through `TCP`. The plan is use `hiredis` as default but choose `TCP` when it's not available.








