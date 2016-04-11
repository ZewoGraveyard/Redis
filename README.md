Redis
======
[![Swift 3.0](https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat)](https://swift.org)
[![License MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat)](https://tldrlegal.com/license/mit-license)
[![Slack Status](https://zewo-slackin.herokuapp.com/badge.svg)](https://zewo-slackin.herokuapp.com)
[![Build Status](https://travis-ci.org/Zewo/Redis.svg?branch=master)](https://travis-ci.org/Zewo/Redis)

A pure Swift client for Redis.

## Features

- [x] Pub/Sub
- [x] Pipeline with `DISCARD`
- [ ] Scripts
- [ ] All commands


## Installation

Then add `Redis` to your `Package.swift`

```swift
import PackageDescription

let package = Package(
    dependencies: [
        .Package(url: "https://github.com/Zewo/Redis", majorVersion: 0, minor: 3)
    ]
)
```

## Using

```swift
let redis = try Redis("127.0.0.1", 6379)
try redis.command(.SET("foo", "bar"))
```

All commands and its parameters are defined in `CommandTypeEnum` enum, with parameters in the same order as Redis docs. The `command` function returns the same response from Redis.

Commands with milliseconds (`SETEX/PSETEX`, `EXPIRE/PEXPIRE`, etc) has a `Bool` parameter to send or return in milliseconds. 
It's always the last parameter.

At this time, there is some commands exceptions:
* `SORT` - To be implemented
* `MIGRATE`, `WAIT`, `SCAN` - These are commands to manage the server. A discussion could be opened to implement it or don't.
* `Server` commands - Same as above

### Pipeline

Pipeline works by issuing commands inside a closure:

```swift
try redis.pipeline {
	try redis.command(.SET("foobar", "foo bar"))
	try redis.command(.SET("foo", "bar"))
}
```

If you need to `WATCH` a key, use the first argument. In case of an error, it'll be returned as `nil`.

```swift
try redis.pipeline(["foo"]) {
	try redis.command(.SET("foobar", "foo bar"))
	try redis.command(.SET("foo", "bar"))
}
```

### PubSub

__WARNING:__ This is a first draft and the syntax is open to discussion. Beware of changes.

PubSub can subscribe to multiple channels at once, but you have to unsubscribe one at time.

```swift
let redis = try Redis("127.0.0.1", 6379)

let pubsub = PubSub(conn: redis)
try pubsub.subscribe(["foo-channel", "bar-channel"]) { message in 
	if message["data"] as? String == "stop" {
		print("Stahp \(message["channel"])!")
		pubsub.unsubscribe(message["channel"] as! String)
	} else {
		print("Keep walking \(message["channel"])...")
	}
}
```

## Contributing

Pull requests are welcome, there is a lot to do.

We recommend using the [Vagrant from redis-py](https://github.com/andymccurdy/redis-py/tree/master/vagrant) to test everything.

## Community

[![Slack](http://s13.postimg.org/ybwy92ktf/Slack.png)](https://zewo-slackin.herokuapp.com)

Join us on [Slack](https://zewo-slackin.herokuapp.com).

License
-------

**Redis** is released under the MIT license. See LICENSE for details.






