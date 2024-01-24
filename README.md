# object_store_ffi

A Rust library that defines an (unofficial) C API for the [object_store](https://github.com/apache/arrow-rs/tree/master/object_store) crate ([docs.rs](https://docs.rs/object_store/latest/object_store/)).

> [!NOTE]
> This library is _not_ a part of object_store or the Apache Arrow project.

To build it you need to have `rust` and `cargo` installed, and then run `cargo build`.

Pre-built binaries are available via the Julia package [object_store_ffi_jll.jl](https://github.com/JuliaBinaryWrappers/object_store_ffi_jll.jl).
The primary user of object_store_ffi is the Julia package [RustyObjectStore.jl](https://github.com/RelationalAI/RustyObjectStore.jl/).

#### Releasing

New releases of object_store_ffi are made by creating and pushing a tag with the new version (format: `v[0-9]+.[0-9]+.[0-9]+`).
This will dispatch a release workflow that will build the assets and create the GitHub Release.

```bash
git tag -a v0.3.0 -m "New Release"
git push --tags
```

Each release should increment the version number following [Semantic Versioning](https://semver.org/) and should be kept in sync with the `version` field of `Cargo.toml`.

Whenever a new release of object_store_ffi is made, we should build binaries and release a new version of object_store_ffi_jll.jl.
This is done by making a pull request to [Yggdrasil](https://github.com/JuliaPackaging/Yggdrasil) to update [the object_store_ffi build recipe](https://github.com/JuliaPackaging/Yggdrasil/blob/master/O/object_store_ffi/build_tarballs.jl).
The PR should usually just update the `GitSource` commit hash to point to the new release, and to update the `version` number.
Merging such a PR will automatically build the binaries for the supported platforms and publish them to the object_store_ffi_jll.jl repository as part of a new release of object_store_ffi_jll.jl.

#### Local build with BinaryBuilder.jl

If you want to try building object_store_ffi_jll.jl locally, this can be done using [BinaryBuilder.jl](https://github.com/JuliaPackaging/BinaryBuilder.jl).
You will need a local copy of the object_store_ffi build recipe `build_tarballs.jl`, and a Julia environment with BinaryBuilder.jl installed.

For example, clone Yggdrasil and navigate to the object_store_ffi directory:

```sh
$ git clone git@github.com:JuliaPackaging/Yggdrasil.git
$ cd Yggdrasil/O/object_store_ffi/
```

Create a Julia environment with BinaryBuilder installed:

```sh
$ julia --project=build_env -e 'using Pkg; Pkg.add("BinaryBuilder")'
```

You can then run the `build_tarballs.jl` script:

```julia
$ julia --project=build_env build_tarballs.jl aarch64-apple-darwin --deploy=local --verbose --debug
```

See `build_tarballs.jl --help` for supported arguments.

See the [BinaryBuilder documentation](https://docs.binarybuilder.org) for more details.

_Note BinaryBuilder requires Docker if running on MacOS or Windows._
