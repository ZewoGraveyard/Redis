import PackageDescription

let package = Package(
    name: "Swift-Redis",
    dependencies: [
        .Package(url: "https://github.com/Zewo/TCP.git", majorVersion: 0, minor: 2),
        .Package(url: "https://github.com/Zewo/String.git", majorVersion: 0, minor: 2)
    ]
)