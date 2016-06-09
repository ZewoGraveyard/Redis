import PackageDescription

let package = Package(
    name: "Redis",
    dependencies: [
        .Package(url: "https://github.com/VeniceX/TCP.git", majorVersion: 0, minor: 7)
    ]
)
