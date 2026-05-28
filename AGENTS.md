## Cursor Cloud specific instructions

### Project overview

FUSE (Fusion Synthesis Engine) is an open-source Julia package for integrated simulations of fusion devices (tokamaks). It is a pure Julia scientific computing library — no web servers, databases, or Docker containers needed. All simulation runs within a Julia REPL/session.

### Julia setup

- Julia is installed via [juliaup](https://github.com/JuliaLang/juliaup). The `julia` binary lives in `~/.juliaup/bin/` — ensure `PATH` includes this directory.
- FUSE depends on a custom Julia registry called **FuseRegistry** (`https://github.com/ProjectTorreyPines/FuseRegistry.jl.git`) in addition to the standard General registry. Both must be registered before instantiating the project.
- To activate the project environment, always use `julia --project=@.` from the workspace root (`/workspace`).

### Running the application

```bash
export PATH="$HOME/.juliaup/bin:$PATH"
export GKSwstype=100  # disables interactive plot windows (required in headless environments)
julia --project=@. -e 'using FUSE; ini, act = FUSE.case_parameters(:FPP); dd = FUSE.init(ini, act); FUSE.ActorStationaryPlasma(dd, act)'
```

### Running tests

```bash
julia --project=@. -e 'using Pkg; Pkg.test()'
```

Or run individual test files:

```bash
julia --project=@. -e 'using FUSE; using Test; include("test/runtests_warmup.jl"); include("test/runtests_basics.jl")'
```

The full test suite (`Pkg.test()`) can take 1+ hour due to the warmup phase (first-run JIT compilation of ~60 physics actors). Subsequent runs within the same Julia session are much faster.

### Linting / formatting

Uses [JuliaFormatter.jl](https://github.com/domluna/JuliaFormatter.jl) with settings in `.JuliaFormatter.toml`.

```bash
julia --project=@. -e 'using JuliaFormatter; format(".")'
```

To check without overwriting:

```bash
julia --project=@. -e 'using JuliaFormatter; println(format("."; overwrite=false) ? "OK" : "Needs formatting")'
```

### Key caveats

- **`GKSwstype=100`** must be set in headless/CI environments to avoid GR/Plots backend errors.
- **First-run latency:** The first `using FUSE` and any actor call in a fresh Julia session triggers significant JIT compilation. This is inherent to Julia and not a bug.
- **Precompilation after dependency changes:** After `Pkg.instantiate()` or `Pkg.update()`, Julia will automatically precompile packages on first load, which can take 10+ minutes.
- The `Makefile` at the repo root contains many useful developer targets — run `make help` for an overview (targets are organized into `@user` and `@devs` categories).
