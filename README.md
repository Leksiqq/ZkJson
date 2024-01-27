**Attention!** _This article, as well as this announcement, are automatically translated from Russian_.

The **Net.Leksi.ZkJson** library allows you to save JSON files in [Apache ZooKeeper](https://zookeeper.apache.org/), as well as load them from `ZooKeeper` using the standard **Microsoft** library [System .Text.Json](https://learn.microsoft.com/en-us/dotnet/api/system.text.json?view=net-8.0).

This feature can be primarily useful when creating and running microservice applications in **Docker**, when the entire configuration is placed in `ZooKeeper`, and the connection string with `ZooKeeper` is passed to the service upon startup, with the corresponding `chroot` specified.

On the other hand, this library can be used to create a utility for uploading the required configurations into `ZooKeeper`.

All classes are contained in the `Net.Leksi.ZkJson` namespace.

- [ZkJsonSerializer](https://github.com/Leksiqq/ZkJson.net/wiki/ZkJsonSerializer-en) - the main class, which is the factory of the corresponding Json converter. Also has properties used for settings.

- [ZkAction](https://github.com/Leksiqq/ZkJson.net/wiki/ZkAction-en) - an enumeration whose elements correspond to the options for updating data in `ZooKeeper`; in the current version only the option of complete replacement is implemented.

- [ZkStub](https://github.com/Leksiqq/ZkJson.net/wiki/ZkStub-en) - a stub class to indicate to the serialization/deserialization processor that the appropriate Json converter should be used.

- [ZkJsonException](https://github.com/Leksiqq/ZkJson.net/wiki/ZkJsonException-en) - an exception.

It is also suggested that you familiarize yourself with the demonstration projects:
- [Demo:ZkJsonDemo](https://github.com/Leksiqq/ZkJson.net/wiki/Demo-ZkJsonDemo) - a utility for reading, writing and deleting `ZooKeeper` data. For simplicity, it is implemented without authentication and authorization.
- [Demo:TestProject1](https://github.com/Leksiqq/ZkJson.net/wiki/Demo-TestProject1) - shows an example usage for writing data generated programmatically using `LINQ`.

NuGet Package: [Net.Leksi.ZkJson](https://www.nuget.org/packages/Net.Leksi.ZkJson/)

